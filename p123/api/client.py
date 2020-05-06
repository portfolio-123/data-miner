import requests
import p123.api.cons as cons
import configparser


class ClientException(Exception):
    pass


class Client(object):
    """
    class for interfacing with P123 API
    """

    def __init__(self, *, api_id, api_key):
        self._api_id = api_id
        self._api_key = api_key
        self._session = requests.Session()

    def auth(self):
        """
        Authenticates and sets the Bearer authorization header on success. This method doesn't need to be called
        explicitly since all requests first check if the authorization header is set and attempt to re-authenticate
        if session expires.
        :return: bool
        """
        try:
            resp = self._session.post(
                url=cons.API_ENDPOINT + cons.API_AUTH_PATH, auth=(self._api_id, self._api_key),
                verify=cons.API_VERIFY_REQUESTS
            )
            if resp.status_code == 200:
                self._session.headers.update({'Authorization': f'Bearer {resp.text}'})
            elif resp.status_code == 406:
                raise ClientException('API authentication failed: user account inactive')
            elif resp.status_code == 402:
                raise ClientException('API authentication failed: paying subscription required')
            else:
                raise ClientException('API authentication failed')
        except Exception as e:
            raise ClientException(e)

    def _req_with_auth_fallback(self, *, name: str, url: str, params, stop: bool = False):
        """
        Request with authentication fallback, used by all requests (except authentication)
        :param name: request action
        :param url: request url
        :param params: request params
        :param stop: flag to stop infinite authentication recursion
        :return: request response object
        """
        resp = None
        if self._session.headers.get('Authorization') is not None:
            try:
                resp = self._session.post(url=url, json=params, verify=cons.API_VERIFY_REQUESTS, timeout=180)
            except Exception as e:
                raise ClientException(e)
        if resp is None or resp.status_code == 403:
            if not stop:
                self.auth()
                return self._req_with_auth_fallback(name=name, url=url, params=params, stop=True)
        elif resp.status_code == 200:
            return resp
        else:
            message = resp.text
            if not message and resp.status_code == 402:
                message = 'Request quota exhausted'
            if message:
                message = ': ' + message
            raise ClientException(f'API request failed{message}')

    def screen_rolling_backtest(self, params: dict):
        """
        Screen rolling backtest
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='screen rolling backtest',
            url=cons.API_ENDPOINT + cons.API_SCREEN_ROLLING_BACKTEST_PATH,
            params=params
        ).json()

    def screen_backtest(self, params: dict):
        """
        Screen backtest
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='screen backtest',
            url=cons.API_ENDPOINT + cons.API_SCREEN_BACKTEST_PATH,
            params=params
        ).json()

    def universe_update(self, params: dict):
        """
        API universe update
        :param params:
        :return:
        """
        self._req_with_auth_fallback(
            name='universe update',
            url=cons.API_ENDPOINT + cons.API_UNIVERSE_PATH,
            params=params
        )

    def rank_update(self, params: dict):
        """
        API ranking system update
        :param params:
        :return:
        """
        self._req_with_auth_fallback(
            name='ranking system update',
            url=cons.API_ENDPOINT + cons.API_RANK_PATH,
            params=params
        )

    def data(self, params: dict):
        """
        Data
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='data',
            url=cons.API_ENDPOINT + cons.API_DATA_PATH,
            params=params
        ).json()

    def rank_ranks(self, params: dict):
        """
        Data
        :param params:
        :return:
        """
        return self._req_with_auth_fallback(
            name='data',
            url=cons.API_ENDPOINT + cons.API_RANK_RANKS_PATH,
            params=params
        ).json()

    def get_api_id(self):
        return self._api_id


def main():
    config = configparser.ConfigParser()
    config.read('../../config.ini')
    client = Client(api_id=config.get('API', 'id'), api_key=config.get('API', 'key'))
    client.auth()
    client.universe_update({'type': 'stock', 'rules': ['ticker("aapl")']})
    # client.update_universe()
    # data = client.screen_rolling_backtest({'screen': {'type': 'stock'}})
    # data = client.update_universe({'type': 'stock', 'rules': ['ticker("aapl")']})
    # data = client.update_rank({'type': 'stock', 'nodes': '<RankingSystem RankType="Higher"></RankingSystem>'})
    # print(data)


if __name__ == '__main__':
    main()
