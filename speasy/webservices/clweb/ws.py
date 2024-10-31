"""
"""

import logging
from datetime import datetime
from typing import Dict, Optional

from ...config import clweb as clweb_cfg
from ...core import AllowedKwargs, EnsureUTCDateTime
from ...core.http import is_server_up
from ...core.cache import CACHE_ALLOWED_KWARGS, Cacheable, CacheCall
from ...core.dataprovider import (GET_DATA_ALLOWED_KWARGS, ParameterRangeCheck)
from ...core.inventory.indexes import (SpeasyIndex, TimetableIndex)
from ...core.proxy import PROXY_ALLOWED_KWARGS, GetProduct, Proxyfiable


from ...products.timetable import TimeTable
from ...products.variable import SpeasyVariable

from ...core.impex import ImpexProvider, ImpexEndpoint
from ...core.impex.parser import to_xmlid


log = logging.getLogger(__name__)

clweb_provider_name = 'clweb'
clweb_capabilities = [ImpexEndpoint.OBSTREE, ImpexEndpoint.GETPARAM, ImpexEndpoint.LISTTT, ImpexEndpoint.GETTT]
clweb_name_mapping = {
    #'parameter': 'var'
}


def _clweb_cache_entry_name(prefix: str, product: str, start_time: str, **kwargs):
    return f"{prefix}/{product}/{start_time}"


def _clweb_get_proxy_parameter_args(start_time: datetime, stop_time: datetime, product: str, **kwargs) -> Dict:
    return {'path': f"{clweb_provider_name}/{product}", 'start_time': f'{start_time.isoformat()}',
            'stop_time': f'{stop_time.isoformat()}',
            'output_format': kwargs.get('output_format', clweb_cfg.output_format.get())}


class CLWeb_Webservice(ImpexProvider):
    def __init__(self):
        ImpexProvider.__init__(self, provider_name=clweb_provider_name, server_url=clweb_cfg.entry_point(),
                               max_chunk_size_days=clweb_cfg.max_chunk_size_days(),
                               capabilities=clweb_capabilities, name_mapping=clweb_name_mapping,
                               username=clweb_cfg.username(), password=clweb_cfg.password())

    @staticmethod
    def is_server_up() -> bool:
        try:
            return is_server_up(url=clweb_cfg.entry_point())
        except:  # lgtm [py/catch-base-exception]
            return False

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention(), is_pure=True)
    def get_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        return super().get_timetable(timetable_id, **kwargs)

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention())
    def get_user_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        return super().get_user_timetable(timetable_id)

    @CacheCall(cache_retention=24 * 60 * 60, is_pure=True)
    def get_obs_data_tree(self) -> str or None:
        return super().get_obs_data_tree()

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention(), is_pure=True)
    def get_timetables_tree(self) -> str or None:
        return super().get_timetables_tree()

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention(), is_pure=True)
    def get_user_timetables_tree(self) -> str or None:
        return super().get_user_timetables_tree()

    def get_product_variables(self, product_id: str or SpeasyIndex):
        product_id = to_xmlid(product_id)
        if product_id in self.flat_inventory.parameters:
            return self.flat_inventory.parameters[product_id].var.split(".")
        if product_id in self.flat_inventory.components:
            return self.flat_inventory.components[product_id].var.split(".")
        return []

    @AllowedKwargs(
        PROXY_ALLOWED_KWARGS + CACHE_ALLOWED_KWARGS + GET_DATA_ALLOWED_KWARGS + ['output_format', 'restricted_period'])
    @EnsureUTCDateTime()
    @ParameterRangeCheck()
    @Cacheable(prefix=clweb_provider_name, version=None, fragment_hours=lambda x: 12,
               entry_name=_clweb_cache_entry_name)
    @Proxyfiable(GetProduct, _clweb_get_proxy_parameter_args)
    def _get_parameter(self, product, start_time, stop_time,
                       extra_http_headers: Dict or None = None, output_format: str or None = None,
                       restricted_period=False, **kwargs) -> Optional[SpeasyVariable]:
        return super()._get_parameter(product, start_time, stop_time, extra_http_headers=extra_http_headers,
                                      output_format=output_format, restricted_period=restricted_period, **kwargs)
