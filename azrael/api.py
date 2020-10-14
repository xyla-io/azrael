import requests

from typing import Optional, Dict, List
from furl import furl

class SnapchatAPI:
  refresh_token: str
  client_id: str
  client_secret: str
  api_version: str
  ad_account: Optional[Dict[str, any]]
  _token: str

  @property
  def ad_account_id(self) -> Optional[str]:
    return None if self.ad_account is None else self.ad_account['id']

  @ad_account_id.setter
  def ad_account_id(self, id: Optional[str]):
    if id is None:
      self.ad_account = None
      return 

    self.ad_account = self.get_ad_account(ad_account_id=id)

  def __init__(self, refresh_token: str, client_id: str, client_secret: str, api_version: str='v1'):
    self.refresh_token = refresh_token
    self.client_id = client_id
    self.client_secret = client_secret
    self.api_version = api_version
    self._token = None

  def _api_call(self, method, endpoint: str, parameters: Dict[str, any]={}, verbose: bool=False, retry: bool=False):
    if self._token is None:
      self._token = self._generate_access_token()

    url = furl('https://adsapi.snapchat.com')
    url.path = '/{api_version}/{endpoint}'.format(
      api_version=self.api_version,
      endpoint=endpoint,
    )

    url.args = parameters
    response = method(
      url=url.url,
      headers={
        'Authorization': 'Bearer {token}'.format(token=self._token)
      }
    )

    if verbose:
      print(url)
      print(response.text)
    
    if not response.status_code == 200 and not retry:
      if retry:
        raise Exception('Could not generate a new and valid access token', response)
      else:
        self._token = None
        return self._api_call(method, endpoint, parameters, verbose, retry=True)

    return response
    
  def _generate_access_token(self) -> str:
    payload = {
      'grant_type': 'refresh_token',
      'code': self.refresh_token, 
      'client_id': self.client_id,
      'client_secret': self.client_secret,
    }
    response = requests.post('https://accounts.snapchat.com/login/oauth2/access_token', data=payload)
    response_json = response.json()
    return response_json['access_token']
  
  def _assert_success(self, response):
    assert response['request_status'] == 'SUCCESS'

  def get(self, endpoint: str, parameters: Dict[str, any]={}, verbose: bool=False):
    return self._api_call(
      method=requests.get,
      endpoint=endpoint,
      parameters=parameters,
      verbose=verbose,
    )
  
  def get_ad_accounts(self, org_id: str) -> List[Dict[str, any]]:
    endpoint = 'organizations/{org_id}/adaccounts'.format(org_id=org_id)
    response = self.get(endpoint=endpoint).json()

    self._assert_success(response)
    return [a['adaccount'] for a in response['adaccounts']]
  
  def get_ad_account(self, ad_account_id: Optional[str]=None) -> Dict[str, any]:
    account_id = self.ad_account_id if not ad_account_id else ad_account_id
    if account_id is None:
      raise ValueError('ad_account_id must be provided')

    endpoint = 'adaccounts/{ad_account_id}'.format(ad_account_id=account_id)
    response = self.get(endpoint=endpoint).json()

    self._assert_success(response)
    return response['adaccounts'][0]['adaccount']
  
  def get_campaigns(self, ad_account_id: Optional[str]=None) -> List[Dict[str, any]]:
    account_id = self.ad_account_id if not ad_account_id else ad_account_id
    if account_id is None:
      return []

    endpoint = 'adaccounts/{ad_account_id}/campaigns'.format(ad_account_id=account_id)
    response = self.get(endpoint=endpoint).json()

    self._assert_success(response)
    return [c['campaign'] for c in response['campaigns']]

  def get_ad_squads(self, ad_account_id: Optional[str]=None) -> List[Dict[str, any]]:
    account_id = self.ad_account_id if not ad_account_id else ad_account_id
    if account_id is None:
      return []

    endpoint = 'adaccounts/{ad_account_id}/adsquads'.format(ad_account_id=account_id)
    response = self.get(endpoint=endpoint).json()

    self._assert_success(response)
    return [a['adsquad'] for a in response['adsquads']]

  def get_ads(self, ad_account_id: Optional[str]=None) -> List[Dict[str, any]]:
    account_id = self.ad_account_id if not ad_account_id else ad_account_id
    if account_id is None:
      return []

    endpoint = 'adaccounts/{ad_account_id}/ads'.format(ad_account_id=account_id)
    response = self.get(endpoint=endpoint).json()

    self._assert_success(response)
    return [a['ad'] for a in response['ads']]

  def get_ad(self, ad_id: str) -> Dict[str, any]:
    endpoint = f'ads/{ad_id}'
    response = self.get(endpoint=endpoint).json()

    self._assert_success(response)
    return response['ad']