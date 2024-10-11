"""
"""

import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from ...config import clweb as clweb_cfg
from ...core import AllowedKwargs, make_utc_datetime, EnsureUTCDateTime
from ...core.cache import CACHE_ALLOWED_KWARGS, Cacheable, CacheCall
from ...core.dataprovider import (GET_DATA_ALLOWED_KWARGS, DataProvider,
                                  ParameterRangeCheck)
from ...core.datetime_range import DateTimeRange
from ...core.inventory.indexes import (ComponentIndex,
                                       DatasetIndex, ParameterIndex,
                                       SpeasyIndex, TimetableIndex)
from ...core.proxy import PROXY_ALLOWED_KWARGS, GetProduct, Proxyfiable


from ...core.impex import ImpexProvider, ImpexClient, ImpexEndpoint, ImpexXMLParser
from ...core.impex.parser import to_xmlid

from ...products.catalog import Catalog
from ...products.dataset import Dataset
from ...products.timetable import TimeTable
from ...products.variable import SpeasyVariable


log = logging.getLogger(__name__)

clweb_provider_name = 'clweb'
clweb_capabilities = [ ImpexEndpoint.OBSTREE, ImpexEndpoint.GETPARAM, ImpexEndpoint.LISTTT, ImpexEndpoint.GETTT]

clweb_client = ImpexClient(capabilities=clweb_capabilities, server_url= clweb_cfg.entry_point(),
                           username=clweb_cfg.username(), password=clweb_cfg.password(), output_format='CDF')

clweb_name_mapping = {
    "dataset": "name"
}
clweb_provider = impex_provider = ImpexProvider(provider=clweb_provider_name,
                                                impex_client=clweb_client,
                                                max_chunk_size_days=clweb_cfg.max_chunk_size_days())


def _clweb_cache_entry_name(prefix: str, product: str, start_time: str, **kwargs):
    return f"{prefix}/{product}/{start_time}"


def _clweb_get_proxy_parameter_args(start_time: datetime, stop_time: datetime, product: str, **kwargs) -> Dict:
    return {'path': f"{clweb_provider_name}/{product}", 'start_time': f'{start_time.isoformat()}',
            'stop_time': f'{stop_time.isoformat()}',
            'output_format': kwargs.get('output_format', clweb_cfg.output_format.get())}


class ProductType(Enum):
    """Enumeration of the type of products available in CL_Webservice.
    """
    UNKNOWN = 0
    DATASET = 1
    PARAMETER = 2
    COMPONENT = 3
    TIMETABLE = 4


class CLWeb_Webservice(DataProvider):
    __datetime_format__ = "%Y-%m-%dT%H:%M:%S.%f"

    def __init__(self):
        DataProvider.__init__(self, provider_name=clweb_provider_name)

    def __del__(self):
        pass

    @staticmethod
    def is_server_up() -> bool:
        return clweb_provider.is_server_up()

    def build_inventory(self, root: SpeasyIndex):
        return clweb_provider.build_inventory(root, clweb_name_mapping)

    def build_private_inventory(self, root: SpeasyIndex):
        return clweb_provider.build_private_inventory(root, clweb_name_mapping)

    def product_version(self, parameter_id: str or ParameterIndex):
        return 0

    def parameter_range(self, parameter_id: str or ParameterIndex) -> Optional[DateTimeRange]:
        return self._parameter_range(parameter_id)

    def dataset_range(self, dataset_id: str or DatasetIndex) -> Optional[DateTimeRange]:
        return self._dataset_range(dataset_id)

    def get_data(self, product, start_time=None, stop_time=None,
                 **kwargs) -> Optional[Union[SpeasyVariable, TimeTable, Catalog, Dataset]]:
        product_t = self.product_type(product)
        if product_t == ProductType.DATASET and start_time and stop_time:
            return self.get_dataset(dataset_id=product, start=start_time, stop=stop_time, **kwargs)
        if product_t == ProductType.PARAMETER and start_time and stop_time:
            return self.get_parameter(product=product, start_time=start_time, stop_time=stop_time, **kwargs)
        if product_t == ProductType.TIMETABLE:
            return self.get_timetable(timetable_id=product, **kwargs)
        raise ValueError(f"Unknown product: {product}")

    def get_user_parameter(self, parameter_id: str or ParameterIndex, start_time: datetime or str,
                           stop_time: datetime or str) -> Optional[SpeasyVariable]:
        parameter_id = to_xmlid(parameter_id)
        start_time, stop_time = make_utc_datetime(start_time), make_utc_datetime(stop_time)
        return clweb_provider.dl_user_parameter(parameter_id=parameter_id, start_time=start_time, stop_time=stop_time)

    def get_parameter(self, product, start_time, stop_time,
                      extra_http_headers: Dict or None = None, **kwargs) -> Optional[
        SpeasyVariable]:
        return self._get_parameter(product, start_time, stop_time, extra_http_headers=extra_http_headers, **kwargs)

    def get_product_variables(self, product_id: str or SpeasyIndex):
        product_id = to_xmlid(product_id)
        if product_id in self.flat_inventory.parameters:
            return self.flat_inventory.parameters[product_id].var.split(".")
        if product_id in self.flat_inventory.components:
            return self.flat_inventory.components[product_id].var.split(".")
        return []

    @AllowedKwargs(
        PROXY_ALLOWED_KWARGS + CACHE_ALLOWED_KWARGS + GET_DATA_ALLOWED_KWARGS)
    @EnsureUTCDateTime()
    @ParameterRangeCheck()
    @Cacheable(prefix=clweb_provider_name, version=product_version, fragment_hours=lambda x: 12,
               entry_name=_clweb_cache_entry_name)
    @Proxyfiable(GetProduct, _clweb_get_proxy_parameter_args)
    def _get_parameter(self, product, start_time, stop_time,
                       extra_http_headers: Dict or None = None, **kwargs) -> \
        Optional[
            SpeasyVariable]:
        log.debug(f'Get data: product = {product}, data start time = {start_time}, data stop time = {stop_time}')
        return clweb_provider.dl_parameter(start_time=start_time, stop_time=stop_time, parameter_id=product,
                                           extra_http_headers=extra_http_headers,
                                           product_variables=self.get_product_variables(product))

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

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention())
    def get_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        return clweb_provider.dl_timetable(to_xmlid(timetable_id), **kwargs)

    def list_parameters(self, dataset_id: Optional[str or DatasetIndex] = None) -> List[ParameterIndex]:
        return list(self.flat_inventory.datasets[to_xmlid(dataset_id)])

    def list_timetables(self) -> List[TimetableIndex]:
        return list(self.flat_inventory.timetables.values())

    def list_datasets(self) -> List[DatasetIndex]:
        return list(self.flat_inventory.datasets.values())

    def _find_parent_dataset(self, product_id: str or DatasetIndex or ParameterIndex or ComponentIndex) -> Optional[
        str]:
        product_id = to_xmlid(product_id)
        product_type = self.product_type(product_id)
        if product_type is ProductType.DATASET:
            return product_id
        elif product_type in (ProductType.COMPONENT, ProductType.PARAMETER):
            for dataset in self.flat_inventory.datasets.values():
                if product_id in dataset:
                    return to_xmlid(dataset)

    def product_type(self, product_id: str or SpeasyIndex) -> ProductType:
        product_id = to_xmlid(product_id)
        if product_id in self.flat_inventory.datasets:
            return ProductType.DATASET
        if product_id in self.flat_inventory.parameters:
            return ProductType.PARAMETER
        if product_id in self.flat_inventory.components:
            return ProductType.COMPONENT
        if product_id in self.flat_inventory.timetables:
            return ProductType.TIMETABLE

        return ProductType.UNKNOWN

    def get_product_variables(self, product_id: str or SpeasyIndex):
        product_id = to_xmlid(product_id)
        if product_id in self.flat_inventory.parameters:
            return self.flat_inventory.parameters[product_id].var.split(".")
        if product_id in self.flat_inventory.components:
            return self.flat_inventory.components[product_id].var.split(".")
        return []
