import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional, List, Union
from types import SimpleNamespace
import warnings
from copy import deepcopy

import numpy as np

from ..datetime_range import DateTimeRange
from ...core.dataprovider import (DataProvider)

from ...core import make_utc_datetime

from ...core.inventory.indexes import ComponentIndex, DatasetIndex, ParameterIndex, SpeasyIndex, \
                                      TimetableIndex, CatalogIndex
from ...core.cdf import load_variables as cdf_load_variables
from ...products.variable import SpeasyVariable, merge, DataContainer
from ...products.catalog import Catalog
from ...products.dataset import Dataset
from ...products.timetable import TimeTable

from ...inventories import flat_inventories

from .parser import ImpexXMLParser, to_xmlid
from .client import ImpexClient, ImpexEndpoint
from .utils import load_catalog, load_timetable, is_private, is_public
from .exceptions import MissingCredentials


log = logging.getLogger(__name__)


class ImpexProductType(Enum):
    """Enumeration of the type of products available in AMDA_Webservice.
    """
    UNKNOWN = 0
    DATASET = 1
    PARAMETER = 2
    COMPONENT = 3
    TIMETABLE = 4
    CATALOG = 5


class ImpexProvider(DataProvider):
    def __init__(self, provider_name: str, server_url: str, max_chunk_size_days: int = 10, capabilities: List = None,
                 username: str = "", password: str = "", name_mapping: Dict = None, output_format: str = 'CDF'):
        self.provider_name = provider_name
        self.server_url = server_url
        self.client = ImpexClient(capabilities=capabilities, server_url=server_url,
                                  username=username, password=password, output_format=output_format)
        if not self.client.is_alive():
            warnings.warn(f"The data provider {provider_name} appears to be under maintenance")
        self.max_chunk_size_days = max_chunk_size_days
        self.name_mapping = name_mapping
        DataProvider.__init__(self, provider_name=provider_name)

    def __del__(self):
        pass

    def build_inventory(self, root: SpeasyIndex):
        obs_data_tree = ImpexXMLParser.parse(self.get_obs_data_tree(), self.provider_name, self.name_mapping)
        root.Parameters = SpeasyIndex(name='Parameters', provider=self.provider_name, uid='Parameters',
                                      meta=obs_data_tree.dataRoot.dataCenter.__dict__)

        if self.client.is_capable(ImpexEndpoint.GETTT):
            root.TimeTables = SpeasyIndex(name='TimeTables', provider=self.provider_name, uid='TimeTables')
            public_tt = ImpexXMLParser.parse(self.get_timetables_tree(), self.provider_name, self.name_mapping)
            if hasattr(public_tt, 'ws'):
                # CLWeb case
                shared_root = public_tt.ws.timetabList
            else:
                # AMDA case
                shared_root = public_tt.timeTableList
            root.TimeTables.SharedTimeTables = SpeasyIndex(name='SharedTimeTables', provider=self.provider_name,
                                                           uid='SharedTimeTables',
                                                           meta=shared_root.__dict__)

        if self.client.is_capable(ImpexEndpoint.GETCAT):
            root.Catalogs = SpeasyIndex(name='Catalogs', provider=self.provider_name, uid='Catalogs')
            public_cat = ImpexXMLParser.parse(self.get_catalogs_tree(), self.provider_name, self.name_mapping)
            root.Catalogs.SharedCatalogs = SpeasyIndex(name='SharedCatalogs', provider=self.provider_name,
                                                       uid='SharedCatalogs',
                                                       meta=public_cat.catalogList.__dict__)

        return root

    def build_private_inventory(self, root: SpeasyIndex):
        if self.client.credential_are_valid():
            if self.client.is_capable(ImpexEndpoint.GETTT):
                user_tt = ImpexXMLParser.parse(self.get_user_timetables_tree(),
                                               self.provider_name, self.name_mapping, is_public=False)
                if hasattr(user_tt, 'ws'):
                    # CLWeb case
                    public_root = user_tt.ws.timetabList
                else:
                    # AMDA case
                    public_root = user_tt.timetabList
                root.TimeTables.MyTimeTables = SpeasyIndex(name='MyTimeTables', provider=self.provider_name,
                                                           uid='MyTimeTables', meta=public_root.__dict__)

            if self.client.is_capable(ImpexEndpoint.GETCAT):
                user_cat = ImpexXMLParser.parse(self.get_user_catalogs_tree(), self.provider_name,
                                                self.name_mapping, is_public=False)
                root.Catalogs.MyCatalogs = SpeasyIndex(name='MyCatalogs', provider=self.provider_name, uid='MyCatalogs',
                                                       meta=user_cat.catalogList.__dict__)

            if self.client.is_capable(ImpexEndpoint.LISTPARAM):
                get_derived_parameter_list_xml = self.client.get_derived_parameter_list()
                user_param = ImpexXMLParser.parse(get_derived_parameter_list_xml,
                                                  self.provider_name, self.name_mapping, is_public=False)
                root.DerivedParameters = SpeasyIndex(name='DerivedParameters', provider=self.provider_name,
                                                     uid='DerivedParameters', meta=user_param.ws.paramList.__dict__)
        return root

    def parameter_range(self, parameter_id: str or ParameterIndex) -> Optional[DateTimeRange]:
        return self._parameter_range(parameter_id)

    def dataset_range(self, dataset_id: str or DatasetIndex) -> Optional[DateTimeRange]:
        return self._dataset_range(dataset_id)

    def is_user_catalog(self, catalog_id: str or CatalogIndex):
        return ImpexProvider.is_user_product(catalog_id, flat_inventories.__dict__[self.provider_name].catalogs)

    def is_user_timetable(self, timetable_id: str or TimetableIndex):
        return ImpexProvider.is_user_product(timetable_id, flat_inventories.__dict__[self.provider_name].timetables)

    def is_user_parameter(self, parameter_id: str or ParameterIndex):
        return ImpexProvider.is_user_product(parameter_id, flat_inventories.__dict__[self.provider_name].parameters)

    def get_obs_data_tree(self) -> str or None:
        return self.client.get_obs_data_tree()

    def get_timetables_tree(self) -> str or None:
        return self.client.get_time_table_list()

    def get_catalogs_tree(self) -> str or None:
        return self.client.get_catalog_list()

    def get_user_timetables_tree(self) -> str or None:
        return self.client.get_time_table_list(use_credentials=True)

    def get_user_catalogs_tree(self) -> str or None:
        return self.client.get_catalog_list(use_credentials=True)

    def get_derived_parameter_tree(self) -> str or None:
        return self.client.get_derived_parameter_list()

    def get_data(self, product, start_time=None, stop_time=None,
                 **kwargs) -> Optional[Union[SpeasyVariable, TimeTable, Catalog, Dataset]]:
        product_t = self.product_type(product)
        if product_t == ImpexProductType.DATASET and start_time and stop_time:
            return self.get_dataset(dataset_id=product, start=start_time, stop=stop_time, **kwargs)
        if product_t == ImpexProductType.PARAMETER and start_time and stop_time:
            if self.is_user_parameter(product):
                return self.get_user_parameter(parameter_id=product,
                                               start_time=start_time, stop_time=stop_time, **kwargs)
            else:
                return self.get_parameter(product=product, start_time=start_time, stop_time=stop_time, **kwargs)
        if product_t == ImpexProductType.CATALOG:
            if self.is_user_catalog(product):
                return self.get_user_catalog(catalog_id=product, **kwargs)
            else:
                return self.get_catalog(catalog_id=product, **kwargs)
        if product_t == ImpexProductType.TIMETABLE:
            if self.is_user_timetable(product):
                return self.get_user_timetable(timetable_id=product, **kwargs)
            else:
                return self.get_timetable(timetable_id=product, **kwargs)
        raise ValueError(f"Unknown product: {product}")

    def get_user_parameter(self, parameter_id: str or ParameterIndex, start_time: datetime or str,
                           stop_time: datetime or str, **kwargs) -> Optional[SpeasyVariable]:
        parameter_id = to_xmlid(parameter_id)
        start_time, stop_time = make_utc_datetime(start_time), make_utc_datetime(stop_time)
        return self.dl_user_parameter(parameter_id=parameter_id, start_time=start_time, stop_time=stop_time, **kwargs)

    def get_parameter(self, product, start_time, stop_time,
                      extra_http_headers: Dict or None = None,
                      output_format: str or None = None, **kwargs) -> Optional[SpeasyVariable]:
        if hasattr(self, 'has_time_restriction') and self.has_time_restriction(product, start_time, stop_time):
            kwargs['disable_proxy'] = True
            kwargs['restricted_period'] = True
            return self._get_parameter(product, start_time, stop_time, extra_http_headers=extra_http_headers,
                                       output_format=output_format or self.client.output_format, **kwargs)
        else:
            return self._get_parameter(product, start_time, stop_time, extra_http_headers=extra_http_headers,
                                       output_format=output_format or self.client.output_format, **kwargs)

    def get_dataset(self, dataset_id: str or DatasetIndex, start: str or datetime, stop: str or datetime,
                    **kwargs) -> Dataset or None:
        ds_range = self.dataset_range(dataset_id)
        if not ds_range.intersect(DateTimeRange(start, stop)):
            log.warning(f"You are requesting {dataset_id} outside of its definition range {ds_range}")
            return None

        dataset_id = to_xmlid(dataset_id)
        name = self.flat_inventory.datasets[dataset_id].name
        meta = {k: v for k, v in self.flat_inventory.datasets[dataset_id].__dict__.items() if
                not isinstance(v, SpeasyIndex)}
        parameters = self.list_parameters(dataset_id)
        return Dataset(name=name,
                       variables={p.name: self.get_parameter(p, start, stop, **kwargs) for p in parameters},
                       meta=meta)

    def get_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        return self.dl_timetable(to_xmlid(timetable_id), **kwargs)

    def get_catalog(self, catalog_id: str or CatalogIndex, **kwargs) -> Optional[Catalog]:
        return self.dl_catalog(to_xmlid(catalog_id), **kwargs)

    def get_user_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        timetable_id = to_xmlid(timetable_id)
        return self.dl_user_timetable(to_xmlid(timetable_id))

    def get_user_catalog(self, catalog_id: str or CatalogIndex, **kwargs) -> Optional[Catalog]:
        catalog_id = to_xmlid(catalog_id)
        return self.dl_user_catalog(to_xmlid(catalog_id), **kwargs)

    def get_product_variables(self, product_id: str or SpeasyIndex):
        product_id = to_xmlid(product_id)
        return [product_id]

    def _get_parameter(self, product, start_time, stop_time,
                       extra_http_headers: Dict or None = None, output_format: str or None = None,
                       restricted_period=False, **kwargs) -> \
        Optional[
            SpeasyVariable]:
        log.debug(f'Get data: product = {product}, data start time = {start_time}, data stop time = {stop_time}')
        return self.dl_parameter(start_time=start_time, stop_time=stop_time, parameter_id=product,
                                 extra_http_headers=extra_http_headers,
                                 output_format=output_format,
                                 product_variables=self.get_product_variables(product),
                                 restricted_period=restricted_period,
                                 time_format='UNIXTIME')

    def dl_parameter_chunk(self, start_time: datetime, stop_time: datetime, parameter_id: str,
                           extra_http_headers: Dict or None = None,
                           use_credentials: bool = False,
                           product_variables: List = None, **kwargs) -> Optional[SpeasyVariable]:
        if not product_variables:
            return None
        url = self.client.get_parameter(start_time=start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                        stop_time=stop_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                        parameter_id=parameter_id, extra_http_headers=extra_http_headers,
                                        use_credentials=use_credentials, **kwargs)
        # check status until done
        if url is not None:
            var = None
            if kwargs.get('output_format', self.client.output_format) in ["CDF_ISTP", "CDF"]:
                var = cdf_load_variables(variables=product_variables, file=url)
            elif hasattr(self, 'load_specific_output_format'):
                var = self.load_specific_output_format(url, product_variables)
            if var is not None:
                if isinstance(var, SpeasyVariable):
                    if len(var):
                        log.debug(
                            f'Loaded var: data shape = {var.values.shape}, data start time = {var.time[0]}, \
                                    data stop time = {var.time[-1]}')
                    else:
                        log.debug('Loaded var: Empty var')
                else:
                    if parameter_id in self.flat_inventory.parameters:
                        name = self.flat_inventory.parameters[parameter_id].spz_name()
                    else:
                        name = parameter_id
                    var = ImpexProvider._concatenate_variables(var, name)
                    if var is None:
                        log.debug(f'Failed to concatenate variables')
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
        get_timetable_url = self.client.get_timetable(timetable_id, use_credentials=use_credentials, **kwargs)
        if get_timetable_url is not None:
            timetable = load_timetable(filename=get_timetable_url)
            if timetable:
                timetable.meta.update(
                    flat_inventories.__dict__[self.provider_name].timetables.get(timetable_id,
                                                                                 SimpleNamespace()).__dict__)
                log.debug(f'Loaded timetable: id = {timetable_id}')  # lgtm[py/clear-text-logging-sensitive-data]
            else:
                log.debug('Got None')
            return timetable
        return None

    def dl_user_timetable(self, timetable_id: str, **kwargs):
        return self.dl_timetable(timetable_id, use_credentials=True, **kwargs)

    def dl_catalog(self, catalog_id: str, use_credentials=False, **kwargs):
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

    def product_type(self, product_id: str or SpeasyIndex) -> ImpexProductType:
        """Returns product type for any known ADMA product from its index or ID.

        Parameters
        ----------
        product_id: str or AMDAIndex
            product id

        Returns
        -------
        ImpexProductType
            Type of product IE ImpexProductType.DATASET, ImpexProductType.TIMETABLE, ...

        Examples
        --------

        >>> import speasy as spz
        >>> spz.amda.product_type("imf")
        <ImpexProductType.PARAMETER: 2>
        >>> spz.amda.product_type("ace-imf-all")
        <ImpexProductType.DATASET: 1>
        """
        product_id = to_xmlid(product_id)
        if product_id in flat_inventories.__dict__[self.provider_name].datasets:
            return ImpexProductType.DATASET
        if product_id in flat_inventories.__dict__[self.provider_name].parameters:
            return ImpexProductType.PARAMETER
        if product_id in flat_inventories.__dict__[self.provider_name].components:
            return ImpexProductType.COMPONENT
        if product_id in flat_inventories.__dict__[self.provider_name].timetables:
            return ImpexProductType.TIMETABLE
        if product_id in flat_inventories.__dict__[self.provider_name].catalogs:
            return ImpexProductType.CATALOG

        return ImpexProductType.UNKNOWN

    def find_parent_dataset(self, product_id: str or DatasetIndex or ParameterIndex or ComponentIndex) -> Optional[str]:
        product_id = to_xmlid(product_id)
        product_type = self.product_type(product_id)
        if product_type is ImpexProductType.DATASET:
            return product_id
        elif product_type in (ImpexProductType.COMPONENT, ImpexProductType.PARAMETER):
            for dataset in flat_inventories.__dict__[self.provider_name].datasets.values():
                if product_id in dataset:
                    return to_xmlid(dataset)

    @staticmethod
    def is_user_product(product_id: str or SpeasyIndex, collection: Dict):
        xmlid = to_xmlid(product_id)
        if xmlid in collection:
            return is_private(collection[xmlid])
        return False

    def list_datasets(self) -> List[DatasetIndex]:
        return list(filter(is_public, flat_inventories.__dict__[self.provider_name].datasets.values()))

    def list_parameters(self, dataset_id: Optional[str or DatasetIndex] = None) -> List[ParameterIndex]:
        if dataset_id is not None:
            return list(flat_inventories.__dict__[self.provider_name].datasets[to_xmlid(dataset_id)])
        return list(filter(is_public, flat_inventories.__dict__[self.provider_name].parameters.values()))

    def list_user_parameters(self) -> List[ParameterIndex]:
        return list(filter(is_private, flat_inventories.__dict__[self.provider_name].parameters.values()))

    def list_timetables(self) -> List[TimetableIndex]:
        return list(filter(is_public, flat_inventories.__dict__[self.provider_name].timetables.values()))

    def list_user_timetables(self) -> List[TimetableIndex]:
        return list(filter(is_private, flat_inventories.__dict__[self.provider_name].timetables.values()))

    def list_catalogs(self) -> List[CatalogIndex]:
        return list(filter(is_public, flat_inventories.__dict__[self.provider_name].catalogs.values()))

    def list_user_catalogs(self) -> List[CatalogIndex]:
        return list(filter(is_private, flat_inventories.__dict__[self.provider_name].catalogs.values()))

    @staticmethod
    def _concatenate_variables(variables: Dict[str, SpeasyVariable], product_id) -> Optional[SpeasyVariable]:
        if len(variables) == 0:
            return None
        elif len(variables) == 1:
            result = list(variables.values())[0].copy()
            result.values.name = product_id
            return result

        axes = []
        columns = []
        values = None
        meta = {}
        for name, variable in variables.items():
            if not axes:
                values = variable.values.copy()
                axes = variable.axes.copy()
                meta = deepcopy(variable.meta)
            else:
                values = np.concatenate((values, variable.values), axis=1)
                axes += [axis.copy() for axis in variable.axes[1:]]
            columns.append(name)

        if 'FIELDNAM' in meta:
            meta['FIELDNAM'] = name

        return SpeasyVariable(
            axes=axes,
            values=DataContainer(values=values, meta=meta, name=product_id, is_time_dependent=True),
            columns=columns)
