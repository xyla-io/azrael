import pytest

from ..api import SnapchatAPI

@pytest.fixture
def api():
  creds = {
    'refresh_token': 'REFRESHTOKEN',
    'client_id': 'CLIENTID',
    'client_secret': 'CLIENTSECRET',
  }
  return SnapchatAPI(**creds)

@pytest.fixture
def ad_account_id():
  return 'ACCOUNTID'

def test_ad_account_exists(api, ad_account_id):
  account = api.get_ad_account(ad_account_id=ad_account_id)
  import pdb; pdb.set_trace()
  assert account is not None

def test_campaigns(api, ad_account_id):
  campaigns = api.get_campaigns(ad_account_id=ad_account_id)
  import pdb; pdb.set_trace()
  assert len(campaigns) > 0

def test_ad_squads(api, ad_account_id):
  ad_sqauds = api.get_ad_squads(ad_account_id=ad_account_id)
  import pdb; pdb.set_trace()
  assert len(ad_sqauds) > 0

def test_ads(api, ad_account_id):
  ads = api.get_ads(ad_account_id=ad_account_id)
  assert len(ads) > 0