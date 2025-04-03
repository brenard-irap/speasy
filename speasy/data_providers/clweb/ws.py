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
from ...core.inventory.indexes import (ParameterIndex, TimetableIndex, SpeasyIndex)
from ...core.proxy import PROXY_ALLOWED_KWARGS, GetProduct, Proxyfiable, Version
from ...products.timetable import TimeTable
from ...products.variable import SpeasyVariable

from ...core.impex import ImpexProvider, ImpexEndpoint, to_xmlid

log = logging.getLogger(__name__)

clweb_provider_name = 'clweb'
clweb_capabilities = [ImpexEndpoint.OBSTREE, ImpexEndpoint.GETPARAM, ImpexEndpoint.LISTTT, ImpexEndpoint.GETTT]

clweb_name_mapping = {
}

CLWEB_MIN_PROXY_VERSION = Version("0.12.1")


def _clweb_cache_entry_name(prefix: str, product: str, start_time: str, **kwargs):
    return f"{prefix}/{product}/{start_time}"


def _clweb_get_proxy_parameter_args(start_time: datetime, stop_time: datetime, product: str, **kwargs) -> Dict:
    return {'path': f"{clweb_provider_name}/{product}", 'start_time': f'{start_time.isoformat()}',
            'stop_time': f'{stop_time.isoformat()}',
            'output_format': kwargs.get('output_format', clweb_cfg.output_format.get())}


class ClWebservice(ImpexProvider):
    def __init__(self):
        ImpexProvider.__init__(self, provider_name=clweb_provider_name, server_url=clweb_cfg.entry_point(),
                               max_chunk_size_days=clweb_cfg.max_chunk_size_days(),
                               capabilities=clweb_capabilities, name_mapping=clweb_name_mapping,
                               username=clweb_cfg.username(), password=clweb_cfg.password(),
                               output_format='CDF', min_proxy_version=CLWEB_MIN_PROXY_VERSION)

    @staticmethod
    def is_server_up():
        """Check if AMDA Webservice is up by sending a dummy request to the AMDA Webservice URL with a short timeout.

        Returns
        -------
        bool
            True if AMDA Webservice is up, False otherwise.

        """
        try:
            return is_server_up(url=clweb_cfg.entry_point())
        except (Exception,):
            pass
        return False

    def _get_product_variables(self, product_id: str or SpeasyIndex, **kwargs):
        product_id = to_xmlid(product_id)
        if product_id in self.flat_inventory.parameters:
            return self.flat_inventory.parameters[product_id].var.split(".")
        if product_id in self.flat_inventory.components:
            return self.flat_inventory.components[product_id].var.split(".")
        return []

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention(), is_pure=True)
    def get_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        """Get timetable data by ID.

        Parameters
        ----------
        timetable_id: str or TimetableIndex
            time table id

        Returns
        -------
        Optional[TimeTable]
            timetable data

        Examples
        --------

        >>> import speasy as spz
        >>> spz.amda.get_timetable("sharedtimeTable_0")
        <TimeTable: FTE_c1>

        """
        return super().get_timetable(timetable_id, **kwargs)

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention())
    def get_user_timetable(self, timetable_id: str or TimetableIndex, **kwargs) -> Optional[TimeTable]:
        """Get user timetable. Raises an exception if user is not authenticated.

        Parameters
        ----------
        timetable_id: str or TimetableIndex
            timetable id

        Returns
        -------
        Optional[TimeTable]
            user timetable

        Examples
        --------
        >>> import speasy as spz
        >>> spz.amda.get_user_timetable("tt_0") # doctest: +SKIP
        <TimeTable: test_alexis>

        Warnings
        --------
            Calling :meth:`~speasy.amda.amda.AMDA_Webservice.get_user_timetable` without having defined AMDA_Webservice
            login credentials will result in a :class:`~speasy.core.impex.exceptions.MissingCredentials`
            exception being raised.

        """
        return super().get_user_timetable(timetable_id)

    @AllowedKwargs(
        PROXY_ALLOWED_KWARGS + CACHE_ALLOWED_KWARGS + GET_DATA_ALLOWED_KWARGS + ['output_format'])
    @EnsureUTCDateTime()
    @ParameterRangeCheck()
    @Cacheable(prefix=clweb_provider_name, version=None, fragment_hours=lambda x: 12,
               entry_name=_clweb_cache_entry_name)
    @Proxyfiable(GetProduct, _clweb_get_proxy_parameter_args, min_version=CLWEB_MIN_PROXY_VERSION)
    def _get_parameter(self, product, start_time, stop_time,
                       extra_http_headers: Dict or None = None, output_format: str or None = None,
                       restricted_period=False, **kwargs) -> \
        Optional[
            SpeasyVariable]:
        """Get parameter data.

        Parameters
        ----------
        product: str or ParameterIndex
            parameter id
        start_time:
            desired data start time
        stop_time:
            desired data stop time
        extra_http_headers: dict
            reserved for internal use
        output_format: str
            request output format in case of success, only CDF_ISTP is supported for now

        Returns
        -------
        Optional[SpeasyVariable]
            product data if available

        Examples
        --------

        >>> import speasy as spz
        >>> import datetime
        >>> imf_data = spz.amda.get_parameter("imf", "2018-01-01", "2018-01-01T01")
        >>> print(imf_data.columns)
        ['imf[0]', 'imf[1]', 'imf[2]']
        >>> print(imf_data.values.shape)
        (225, 3)

        """
        return super()._get_parameter(product, start_time, stop_time, output_format=output_format,
                                      extra_http_headers=extra_http_headers, **kwargs)

    @CacheCall(cache_retention=24 * 60 * 60, is_pure=True)
    def _get_obs_data_tree(self) -> str or None:
        return super()._get_obs_data_tree()

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention(), is_pure=True)
    def _get_timetables_tree(self) -> str or None:
        return super()._get_timetables_tree()

    @CacheCall(cache_retention=clweb_cfg.user_cache_retention(), is_pure=True)
    def _get_user_timetables_tree(self) -> str or None:
        return super()._get_user_timetables_tree()
