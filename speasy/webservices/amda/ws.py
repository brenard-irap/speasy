"""
"""

import logging
from datetime import datetime
from typing import Dict, Optional

from ...config import amda as amda_cfg
from ...core import AllowedKwargs, make_utc_datetime, EnsureUTCDateTime
from ...core.http import is_server_up
from ...core.cache import CACHE_ALLOWED_KWARGS, Cacheable, CacheCall
from ...core.dataprovider import (GET_DATA_ALLOWED_KWARGS, ParameterRangeCheck)
from ...core.datetime_range import DateTimeRange
from ...core.inventory.indexes import (CatalogIndex, ParameterIndex,
                                       SpeasyIndex, TimetableIndex)
from ...core.proxy import PROXY_ALLOWED_KWARGS, GetProduct, Proxyfiable
from ...products.catalog import Catalog
from ...products.timetable import TimeTable
from ...products.variable import SpeasyVariable

from ...core.impex import ImpexProvider, ImpexEndpoint
from .utils import load_csv

log = logging.getLogger(__name__)

amda_provider_name = 'amda'
amda_capabilities = [ImpexEndpoint.AUTH, ImpexEndpoint.OBSTREE, ImpexEndpoint.GETPARAM, ImpexEndpoint.LISTTT,
                     ImpexEndpoint.GETTT, ImpexEndpoint.LISTCAT, ImpexEndpoint.GETCAT, ImpexEndpoint.LISTPARAM]
amda_name_mapping = {
    "dataset": "xmlid",
    "parameter": "xmlid",
    "folder": "name",
    "component": "xmlid"
}


def _amda_cache_entry_name(prefix: str, product: str, start_time: str, **kwargs):
    output_format: str = kwargs.get('output_format', 'csv')
    if output_format.lower() == 'cdf_istp':
        return f"{prefix}/{product}-cdf_istp/{start_time}"
    else:
        return f"{prefix}/{product}/{start_time}"


def _amda_get_proxy_parameter_args(start_time: datetime, stop_time: datetime, product: str, **kwargs) -> Dict:
    return {'path': f"{amda_provider_name}/{product}", 'start_time': f'{start_time.isoformat()}',
            'stop_time': f'{stop_time.isoformat()}',
            'output_format': kwargs.get('output_format', amda_cfg.output_format.get())}


class AMDA_Webservice(ImpexProvider):
    def __init__(self):
        ImpexProvider.__init__(self, provider_name=amda_provider_name, server_url=amda_cfg.entry_point()+"/php/rest",
                               max_chunk_size_days=amda_cfg.max_chunk_size_days(),
                               capabilities=amda_capabilities, name_mapping=amda_name_mapping,
                               username=amda_cfg.username(), password=amda_cfg.password(),
                               output_format=amda_cfg.output_format())

    @staticmethod
    def is_server_up():
        try:
            return is_server_up(url=amda_cfg.entry_point())
        except:  # lgtm [py/catch-base-exception]
            return False

    def has_time_restriction(self, product_id: str or SpeasyIndex, start_time: str or datetime,
                             stop_time: str or datetime):
        dataset = self.find_parent_dataset(product_id)
        if dataset:
            dataset = self.flat_inventory.datasets[dataset]
            if hasattr(dataset, 'timeRestriction'):
                lower = make_utc_datetime(dataset.timeRestriction)
                upper = make_utc_datetime(dataset.stop_date)
                if lower < upper:
                    return DateTimeRange(lower, upper).intersect(
                        DateTimeRange(start_time, stop_time))
        return False

    def product_version(self, parameter_id: str or ParameterIndex):
        dataset = self.find_parent_dataset(parameter_id)
        return self.flat_inventory.datasets[dataset].lastUpdate

    def load_specific_output_format(self, filename: str, expected_parameter: str):
        return load_csv(filename, expected_parameter)

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_timetable(self, timetable_id: str, **kwargs) -> str or None:
        return super().get_timetable(timetable_id, **kwargs)

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_catalog(self, catalog_id: str, **kwargs) -> str or None:
        return super().get_catalog(catalog_id, **kwargs)

    @CacheCall(cache_retention=amda_cfg.user_cache_retention())
    def get_user_timetable(self, timetable_id: str or TimetableIndex) -> Optional[TimeTable]:
        return super().get_user_timetable(timetable_id)

    @CacheCall(cache_retention=amda_cfg.user_cache_retention())
    def get_user_catalog(self, catalog_id: str or CatalogIndex) -> Optional[Catalog]:
        return super().get_user_catalog(catalog_id)

    @CacheCall(cache_retention=24 * 60 * 60, is_pure=True)
    def get_obs_data_tree(self) -> str or None:
        return super().get_obs_data_tree()

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_timetables_tree(self) -> str or None:
        return super().get_timetables_tree()

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_user_timetables_tree(self) -> str or None:
        return super().get_user_timetables_tree()

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_catalogs_tree(self) -> str or None:
        return super().get_catalogs_tree()

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_user_catalogs_tree(self) -> str or None:
        return super().get_user_catalogs_tree()

    @CacheCall(cache_retention=amda_cfg.user_cache_retention(), is_pure=True)
    def get_derived_parameter_tree(self):
        return super().get_derived_parameter_tree()

    @AllowedKwargs(
        PROXY_ALLOWED_KWARGS + CACHE_ALLOWED_KWARGS + GET_DATA_ALLOWED_KWARGS + ['output_format', 'restricted_period'])
    @EnsureUTCDateTime()
    @ParameterRangeCheck()
    @Cacheable(prefix=amda_provider_name, version=product_version, fragment_hours=lambda x: 12,
               entry_name=_amda_cache_entry_name)
    @Proxyfiable(GetProduct, _amda_get_proxy_parameter_args)
    def _get_parameter(self, product, start_time, stop_time,
                       extra_http_headers: Dict or None = None, output_format: str or None = None,
                       restricted_period=False, **kwargs) -> \
        Optional[
            SpeasyVariable]:
        return super()._get_parameter(product, start_time, stop_time, extra_http_headers=extra_http_headers,
                                      output_format=output_format, restricted_period=restricted_period, **kwargs)
