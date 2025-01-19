from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Callable, Dict, Optional

import requests


class QuickBaseAPI(ABC):
    """
    Abstract base class for interacting with the QuickBase API.

    Provides API initialization through API token, QuickBase URL, realm, and headers.
    Includes properties to access headers and QuickBase base URL.

    Subclasses must implement the 'execute' method, which should return a 'Response' NamedTuple,
    and also define the 'Response' NamedTuple to fit their specific needs.
    """

    def __init__(self, api_token: str):
        self._api_token = api_token
        self._qb_url = "https://api.quickbase.com/v1"
        self._qb_realm = "example.quickbase.com"
        self._headers = {
            "QB-Realm-Hostname": self._qb_realm,
            "User-Agent": f"example_{self.__class__.__name__}_v1.0-dev",
            "Authorization": f"QB-USER-TOKEN {self._api_token}",
            "Content-Type": "application/json",
        }

    def __repr__(self):
        return f"{self.__class__.__name__}(qb_host={self._qb_url}, qb_realm={self._qb_realm})"

    @property
    def headers(self) -> dict:
        return self._headers

    @property
    def qb_url(self) -> str:
        return self._qb_url

    def _build_url(self, endpoint: str) -> str:
        """
        Build a full API URL from a given endpoint.
        """
        return f"{self._qb_url}/{endpoint}"

    def _get(self, endpoint: str, params: Optional[dict] = None) -> requests.Response:
        """
        Perform a GET request to the specified endpoint with optional params.
        """
        url = self._build_url(endpoint)
        return requests.get(url=url, params=params, headers=self._headers)

    def _post(self, endpoint: str, data: Optional[dict] = None) -> requests.Response:
        """
        Perform a POST request to the specified endpoint with optional data.
        """
        url = self._build_url(endpoint)
        return requests.post(url=url, headers=self._headers, json=data)

    class APIResponse:
        def __init__(self, status: int, data: Callable[[], dict]):
            self._status = status
            self._data = data
            self._loaded_data = None

        @property
        def status(self):
            return self._status

        @property
        def data(self):
            if self._loaded_data is None:
                self._loaded_data = self._data()
            return self._loaded_data

    @abstractmethod
    @lru_cache(maxsize=128)
    def execute(self) -> "QuickBaseAPI.APIResponse":
        pass


class GetAppQB(QuickBaseAPI):
    def __init__(self, api_token: str, app_id: str):
        super().__init__(api_token)
        self._app_id = app_id

    def execute(self):
        response = self._get(f"apps/{self._app_id}")
        return self.APIResponse(
            status=response.status_code,
            data=response.json,
        )


class InsertUpdateRecordQB(QuickBaseAPI):
    def __init__(self, api_token: str, query: Dict[str, Any]):
        super().__init__(api_token)
        self._query = query

    def execute(self):
        response = self._post("records", self._query)
        return self.APIResponse(
            status=response.status_code,
            data=response.json,
        )
