import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from types import SimpleNamespace

from ...core.inventory.indexes import SpeasyIndex
from ...core.cdf import load_variable as cdf_load_variable
from ...products.variable import SpeasyVariable, merge

from ...inventories import flat_inventories

from .parser import ImpexXMLParser, to_xmlid
from .client import ImpexClient, ImpexEndpoint
from .utils import load_catalog, load_timetable
from .exceptions import MissingCredentials


log = logging.getLogger(__name__)


class ImpexProvider:
    def __init__(self, provider: str, impex_client: ImpexClient, max_chunk_size_days: int = 10):
        self.provider = provider
        self.client = impex_client
        self.max_chunk_size_days = max_chunk_size_days

    def is_server_up(self):
        if not self.client.reachable():
            return False
        if self.client.is_capable(ImpexEndpoint.ISALIVE):
            if not self.client.is_alive():
                return False
        return True

    def build_inventory(self, root: SpeasyIndex, name_mapping=None):
        obs_data_tree = ImpexXMLParser.parse(self.client.get_obs_data_tree(), self.provider, name_mapping)
        root.Parameters = SpeasyIndex(name='Parameters', provider=self.provider, uid='Parameters',
                                      meta=obs_data_tree.dataRoot.dataCenter.__dict__)

        if self.client.is_capable(ImpexEndpoint.GETTT):
            root.TimeTables = SpeasyIndex(name='TimeTables', provider=self.provider, uid='TimeTables')
            public_tt = ImpexXMLParser.parse(self.client.get_time_table_list(), self.provider, name_mapping)
            root.TimeTables.SharedTimeTables = SpeasyIndex(name='SharedTimeTables', provider=self.provider,
                                                           uid='SharedTimeTables',
                                                           meta=public_tt.ws.timetabList.__dict__)

        if self.client.is_capable(ImpexEndpoint.GETCAT):
            root.Catalogs = SpeasyIndex(name='Catalogs', provider=self.provider, uid='Catalogs')
            public_cat = ImpexXMLParser.parse(self.client.get_catalog_list(), self.provider, name_mapping)
            root.Catalogs.SharedCatalogs = SpeasyIndex(name='SharedCatalogs', provider=self.provider,
                                                       uid='SharedCatalogs',
                                                       meta=public_cat.catalogList.__dict__)

        return root

    def build_private_inventory(self, root: SpeasyIndex, name_mapping=None):
        if self.client.credential_are_valid():
            if self.client.is_capable(ImpexEndpoint.GETTT):
                user_tt = ImpexXMLParser.parse(self.client.get_time_table_list(use_credentials=True), self.provider,
                                               name_mapping, is_public=False)
                root.TimeTables.MyTimeTables = SpeasyIndex(name='MyTimeTables', provider=self.provider,
                                                           uid='MyTimeTables', meta=user_tt.ws.timetabList.__dict__)

            if self.client.is_capable(ImpexEndpoint.GETCAT):
                user_cat = ImpexXMLParser.parse(self.client.get_catalog_list(use_credentials=True), self.provider,
                                                name_mapping, is_public=False)
                root.Catalogs.MyCatalogs = SpeasyIndex(name='MyCatalogs', provider=self.provider, uid='MyCatalogs',
                                                       meta=user_cat.catalogList.__dict__)

            if self.client.is_capable(ImpexEndpoint.LISTPARAM):
                user_param = ImpexXMLParser.parse(self.client.get_derived_parameter_list(use_credentials=True),
                                                  self.provider, name_mapping, is_public=False)
                root.DerivedParameters = SpeasyIndex(name='DerivedParameters', provider=self.provider,
                                                     uid='DerivedParameters', meta=user_param.ws.paramList.__dict__)
        return root

    def load_specific_output_format(self, filename: str, expected_parameter: str):
        return None

    def dl_parameter_chunk(self, start_time: datetime, stop_time: datetime, parameter_id: str,
                           extra_http_headers: Dict or None = None,
                           use_credentials: bool = False,
                           product_variables: List = None, **kwargs) -> Optional[SpeasyVariable]:
        url = self.client.get_parameter(start_time=start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                        stop_time=stop_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                        parameter_id=parameter_id, extra_http_headers=extra_http_headers,
                                        use_credentials=use_credentials, **kwargs)
        # check status until done
        if url is not None:
            if self.client.output_format in ["CDF_ISTP", "CDF"]:
                var = cdf_load_variable(variable=product_variables[0], file=url)
            else:
                var = self.load_specific_output_format(url, parameter_id)
            if var is not None:
                if len(var):
                    log.debug(
                        f'Loaded var: data shape = {var.values.shape}, data start time = {var.time[0]}, \
                                data stop time = {var.time[-1]}')
                else:
                    log.debug('Loaded var: Empty var')
            else:
                log.debug(f'Failed to load file f{url}')
            return var
        return None

    def dl_parameter(self, start_time: datetime, stop_time: datetime, parameter_id: str,
                     extra_http_headers: Dict or None = None, restricted_period=False,
                     use_credentials: bool = True,
                     product_variables: List = None, **kwargs) -> Optional[SpeasyVariable]:
        dt = timedelta(days=self.max_chunk_size_days)
        if restricted_period:
            if not self.client.credential_are_valid():
                raise MissingCredentials(
                    "Restricted period requested but no credentials provided, please add your "
                    "{} credentials.".format(self.provider))
        if stop_time - start_time > dt:
            var = None
            curr_t = start_time
            while curr_t < stop_time:
                var = merge([var, self.dl_parameter_chunk(curr_t, min(curr_t + dt, stop_time), parameter_id,
                                                          extra_http_headers=extra_http_headers,
                                                          product_variables=product_variables, **kwargs)])
                curr_t += dt
            return var
        else:
            return self.dl_parameter_chunk(start_time, stop_time, parameter_id, extra_http_headers=extra_http_headers,
                                           use_credentials=restricted_period or use_credentials,
                                           product_variables=product_variables, **kwargs)

    def dl_user_parameter(self, start_time: datetime, stop_time: datetime, parameter_id: str,
                          **kwargs) -> Optional[SpeasyVariable]:
        return self.dl_parameter(parameter_id=parameter_id, start_time=start_time, stop_time=stop_time,
                                 use_credentials=True, **kwargs)

    def dl_timetable(self, timetable_id: str, use_credentials=False, **kwargs):
        url = self.client.get_timetable(timetable_id, use_credentials=use_credentials, **kwargs)
        if url is not None:
            timetable = load_timetable(filename=url)
            if timetable:
                timetable.meta.update(flat_inventories.amda.timetables.get(timetable_id, SimpleNamespace()).__dict__)
                log.debug(f'Loaded timetable: id = {timetable_id}')  # lgtm[py/clear-text-logging-sensitive-data]
            else:
                log.debug('Got None')
            return timetable
        return None

    def dl_user_timetable(self, timetable_id: str, **kwargs):
        return self.dl_timetable(timetable_id, use_credentials=True, **kwargs)

    def dl_catalog(self, catalog_id: str, use_credentials=False, **kwargs):
        url = self.client.get_catalog(catID=catalog_id, use_credentials=use_credentials, **kwargs)
        if url is not None:
            catalog = load_catalog(url)
            if catalog:
                log.debug(f'Loaded catalog: id = {catalog_id}')  # lgtm[py/clear-text-logging-sensitive-data]
                catalog.meta.update(flat_inventories.amda.catalogs.get(catalog_id, SimpleNamespace()).__dict__)
            else:
                log.debug('Got None')
            return catalog
        return None

    def dl_user_catalog(self, catalog_id: str, **kwargs):
        return self.dl_catalog(catalog_id, use_credentials=True, **kwargs)
