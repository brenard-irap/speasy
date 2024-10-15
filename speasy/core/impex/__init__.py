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
    def __init__(self, provider_name: str, server_url: str,
                 max_chunk_size_days: int = 10, capabilities: List = None,
                 username: str = "", password: str = ""):
        self.provider_name = provider_name
        self.server_url = server_url
        self.client = ImpexClient(capabilities=capabilities, server_url=server_url,
                                  username=username, password=password, output_format='CDF')
        self.max_chunk_size_days = max_chunk_size_days
        self.cache_handlers = {}

    def set_cache_handlers(self, cache_handlers):
        self.cache_handlers = cache_handlers

    def is_server_up(self):
        if not self.client.reachable():
            return False
        if self.client.is_capable(ImpexEndpoint.ISALIVE):
            if not self.client.is_alive():
                return False
        return True

    def build_inventory(self, root: SpeasyIndex, name_mapping=None):
        if 'get_obs_data_tree' in self.cache_handlers:
            obs_data_tree_xml = self.cache_handlers['get_obs_data_tree']()
        else:
            obs_data_tree_xml = self.client.get_obs_data_tree()
        obs_data_tree = ImpexXMLParser.parse(obs_data_tree_xml, self.provider_name, name_mapping)
        root.Parameters = SpeasyIndex(name='Parameters', provider=self.provider_name, uid='Parameters',
                                      meta=obs_data_tree.dataRoot.dataCenter.__dict__)

        if self.client.is_capable(ImpexEndpoint.GETTT):
            if 'get_time_table_list' in self.cache_handlers:
                get_time_table_list_xml = self.cache_handlers['get_time_table_list']()
            else:
                get_time_table_list_xml = self.client.get_time_table_list()
            root.TimeTables = SpeasyIndex(name='TimeTables', provider=self.provider_name, uid='TimeTables')
            public_tt = ImpexXMLParser.parse(get_time_table_list_xml, self.provider_name, name_mapping)
            if hasattr(public_tt, 'ws'):
                shared_root = public_tt.ws.timetabList # CLWeb case
            else:
                shared_root = public_tt.timeTableList # AMDA case
            root.TimeTables.SharedTimeTables = SpeasyIndex(name='SharedTimeTables', provider=self.provider_name,
                                                           uid='SharedTimeTables',
                                                           meta=shared_root.__dict__)

        if self.client.is_capable(ImpexEndpoint.GETCAT):
            if 'get_catalog_list' in self.cache_handlers:
                get_catalog_list_xml = self.cache_handlers['get_catalog_list']()
            else:
                get_catalog_list_xml = self.client.get_catalog_list()
            root.Catalogs = SpeasyIndex(name='Catalogs', provider=self.provider_name, uid='Catalogs')
            public_cat = ImpexXMLParser.parse(get_catalog_list_xml, self.provider_name, name_mapping)
            root.Catalogs.SharedCatalogs = SpeasyIndex(name='SharedCatalogs', provider=self.provider_name,
                                                       uid='SharedCatalogs',
                                                       meta=public_cat.catalogList.__dict__)

        return root

    def build_private_inventory(self, root: SpeasyIndex, name_mapping=None):
        if self.client.credential_are_valid():
            if self.client.is_capable(ImpexEndpoint.GETTT):
                if 'get_user_time_table_list' in self.cache_handlers:
                    get_user_time_table_list_xml = self.cache_handlers['get_user_time_table_list']()
                else:
                    get_user_time_table_list_xml = self.client.get_time_table_list(use_credentials=True)
                user_tt = ImpexXMLParser.parse(get_user_time_table_list_xml,
                                               self.provider_name, name_mapping, is_public=False)
                if hasattr(user_tt, 'ws'):
                    public_root = user_tt.ws.timetabList  # CLWeb case
                else:
                    public_root = user_tt.timetabList  # AMDA case
                root.TimeTables.MyTimeTables = SpeasyIndex(name='MyTimeTables', provider=self.provider_name,
                                                           uid='MyTimeTables', meta=public_root.__dict__)

            if self.client.is_capable(ImpexEndpoint.GETCAT):
                if 'get_user_catalog_list' in self.cache_handlers:
                    get_user_catalog_list_xml = self.cache_handlers['get_user_catalog_list']()
                else:
                    get_user_catalog_list_xml = self.client.get_catalog_list(use_credentials=True)
                user_cat = ImpexXMLParser.parse(get_user_catalog_list_xml, self.provider_name,
                                                name_mapping, is_public=False)
                root.Catalogs.MyCatalogs = SpeasyIndex(name='MyCatalogs', provider=self.provider_name, uid='MyCatalogs',
                                                       meta=user_cat.catalogList.__dict__)

            if self.client.is_capable(ImpexEndpoint.LISTPARAM):
                user_param = ImpexXMLParser.parse(self.client.get_derived_parameter_list(use_credentials=True),
                                                  self.provider_name, name_mapping, is_public=False)
                root.DerivedParameters = SpeasyIndex(name='DerivedParameters', provider=self.provider_name,
                                                     uid='DerivedParameters', meta=user_param.ws.paramList.__dict__)
        return root

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
            if kwargs.get('output_format', self.client.output_format) in ["CDF_ISTP", "CDF"]:
                var = cdf_load_variable(variable=product_variables[0], file=url)
            elif hasattr(self, 'load_specific_output_format'):
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
                     use_credentials: bool = False,
                     product_variables: List = None, **kwargs) -> Optional[SpeasyVariable]:
        dt = timedelta(days=self.max_chunk_size_days)
        if restricted_period:
            if not self.client.credential_are_valid():
                raise MissingCredentials(
                    "Restricted period requested but no credentials provided, please add your "
                    "{} credentials.".format(self.provider_name))
            else:
                use_credentials = True
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
        if 'get_timetable' in self.cache_handlers:
            get_timetable_url = self.cache_handlers['get_timetable'](timetable_id,
                                                                     use_credentials=use_credentials, **kwargs)
        else:
            get_timetable_url = self.client.get_timetable(timetable_id, use_credentials=use_credentials, **kwargs)
        if get_timetable_url is not None:
            timetable = load_timetable(filename=get_timetable_url)
            if timetable:
                timetable.meta.update(
                    flat_inventories.__dict__[self.provider_name].timetables.get(timetable_id, SimpleNamespace()).__dict__)
                log.debug(f'Loaded timetable: id = {timetable_id}')  # lgtm[py/clear-text-logging-sensitive-data]
            else:
                log.debug('Got None')
            return timetable
        return None

    def dl_user_timetable(self, timetable_id: str, **kwargs):
        return self.dl_timetable(timetable_id, use_credentials=True, **kwargs)

    def dl_catalog(self, catalog_id: str, use_credentials=False, **kwargs):
        if 'get_catalog' in self.cache_handlers:
            get_catalog_url = self.cache_handlers['get_catalog'](catalog_id,
                                                                 use_credentials=use_credentials, **kwargs)
        else:
            get_catalog_url = self.client.get_catalog(catalog_id, use_credentials=use_credentials, **kwargs)
        if get_catalog_url is not None:
            catalog = load_catalog(get_catalog_url)
            if catalog:
                log.debug(f'Loaded catalog: id = {catalog_id}')  # lgtm[py/clear-text-logging-sensitive-data]
                catalog.meta.update(
                    flat_inventories.__dict__[self.provider_name].catalogs.get(catalog_id, SimpleNamespace()).__dict__)
            else:
                log.debug('Got None')
            return catalog
        return None

    def dl_user_catalog(self, catalog_id: str, **kwargs):
        return self.dl_catalog(catalog_id, use_credentials=True, **kwargs)
