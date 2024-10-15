from enum import Enum
from typing import Dict
from json.decoder import JSONDecodeError
import logging
import time

from ...core.http import is_server_up
from ...core import http

from .exceptions import MissingCredentials

log = logging.getLogger(__name__)


class ImpexEndpoint(Enum):
    """Impex API endpoints.
    """
    AUTH = "auth.php"
    OBSTREE = "getObsDataTree.php"
    LISTTT = "getTimeTablesList.php"
    LISTCAT = "getCatalogsList.php"
    LISTPARAM = "getParameterList.php"
    GETTT = "getTimeTable.php"
    GETCAT = "getCatalog.php"
    GETPARAM = "getParameter.php"
    GETSTATUS = "getStatus.php"
    ISALIVE = "isAlive.php"


class ImpexClient:
    def __init__(self, server_url="", capabilities=None, username="", password="",
                 output_format="ASCII", time_format='UNIXTIME'):
        self.server_url = server_url
        if capabilities is None:
            capabilities = [ImpexEndpoint.OBSTREE, ImpexEndpoint.GETPARAM]
        self.capabilities = capabilities
        self.username = username
        self.password = password
        self.output_format = output_format
        self.time_format = time_format
        self.use_token = self.is_capable(ImpexEndpoint.AUTH)

    def is_capable(self, api: ImpexEndpoint):
        return api in self.capabilities

    def credential_are_valid(self):
        return self.username != "" and self.password != ""

    def get_credentials(self):
        if self.credential_are_valid():
            return self.username, self.password
        else:
            raise MissingCredentials()

    def reachable(self):
        try:
            return is_server_up(url=f"{self.server_url}/")
        except:  # lgtm [py/catch-base-exception]
            return False

    def is_alive(self):
        pass

    def auth(self):
        return self._send_request(ImpexEndpoint.AUTH)

    def get_obs_data_tree(self, use_credentials=False):
        params = {}
        if use_credentials:
            params['userID'], params['password'] = self.get_credentials()
        return self._send_indirect_request(ImpexEndpoint.OBSTREE, params=params)

    def get_time_table_list(self, use_credentials=False):
        params = {}
        if use_credentials:
            params['userID'], params['password'] = self.get_credentials()
        return self._send_indirect_request(ImpexEndpoint.LISTTT, params=params)

    def get_catalog_list(self, use_credentials=False):
        params = {}
        if use_credentials:
            params['userID'], params['password'] = self.get_credentials()
        return self._send_indirect_request(ImpexEndpoint.LISTCAT, params=params)

    def get_derived_parameter_list(self, use_credentials=False):
        pass

    def get_status(self):
        pass

    def get_parameter(self, start_time, stop_time, parameter_id, extra_http_headers=None,
                      use_credentials=False, **kwargs):
        params = {
            'startTime': start_time,
            'stopTime': stop_time,
            'parameterID': parameter_id,
            'outputFormat': kwargs.get('output_format', self.output_format)
        }

        if kwargs.get('time_format'):
            params['timeFormat'] = kwargs.get('time_format')

        if use_credentials:
            params['userID'], params['password'] = self.get_credentials()
        if self.use_token:
            params['token'] = self.auth()
        return self._send_request(ImpexEndpoint.GETPARAM, params=params,
                                  extra_http_headers=extra_http_headers)

    def get_timetable(self, tt_id, use_credentials=False, **kwargs):
        params = {
            'ttID': tt_id
        }
        if use_credentials:
            params['userID'], params['password'] = self.get_credentials()
        return self._send_request(ImpexEndpoint.GETTT, params=params)

    def get_catalog(self, catalog_id, use_credentials=False, **kwargs):
        params = {
            'catID': catalog_id
        }
        if use_credentials:
            params['userID'], params['password'] = self.get_credentials()
        return self._send_request(ImpexEndpoint.GETCAT, params=params)

    def _request_url(self, endpoint: ImpexEndpoint) -> str:
        if isinstance(endpoint, ImpexEndpoint):
            return f"{self.server_url}/{endpoint.value}"
        else:
            raise TypeError(f"You must provide an {ImpexEndpoint} instead of {type(endpoint)}")

    def _send_indirect_request(self, endpoint: ImpexEndpoint, params: dict = None,
                               timeout: int = http.DEFAULT_TIMEOUT) -> str or None:
        next_url = self._send_request(endpoint=endpoint, params=params, timeout=timeout)
        if '<' in next_url and '>' in next_url:
            next_url = next_url.split(">")[1].split("<")[0]
        r = http.get(next_url, timeout=timeout)
        if r.status_code == 200:
            return r.text.strip()
        return None

    def _send_request(self, endpoint: ImpexEndpoint, params: Dict = None, timeout: int = http.DEFAULT_TIMEOUT,
                      extra_http_headers: Dict or None = None) -> str or None:
        url = self._request_url(endpoint)
        params = params or {}
        http_headers = extra_http_headers or {}
        # params['token'] = token(server_url=server_url)
        r = http.get(url, params=params, headers=http_headers, timeout=timeout)
        if r.status_code != 200:
            log.debug(f"Failed: {r.status_code}")
            return None
        try:
            js = r.json()
            if 'success' in js and (js['success'] is True) and ('dataFileURLs' in js):
                log.debug(f"success: {js['dataFileURLs']}")
                return js['dataFileURLs']
            elif "success" in js and (js["success"] is True) and ("status" in js) and \
                (js["status"] == "in progress") and self.is_capable(ImpexEndpoint.GETSTATUS):
                log.warning("This request duration is too long, consider reducing time range")
                while True:
                    default_sleep_time = 10.
                    time.sleep(default_sleep_time)
                    url = self._request_url(ImpexEndpoint.GETSTATUS)

                    status = http.get(url, params=js, headers=http_headers).json()
                    if status is not None and status["status"] == "done":
                        return status["dataFileURLs"]
            else:
                log.debug(f"Failed: {r.text}")
        except JSONDecodeError:
            return r.text.strip()
        return None
