import pytest

from ..api import SnapchatAPI
from ..mutating import SnapchatCampaignPauseMutator, SnapchatCampaignBudgetMutator

api_credentials = {
    'refresh_token': 'REFRESHTOKEN',
    'client_id': 'CLIENTID',
    'client_secret': 'CLIENTSECRET',
}

@pytest.fixture
def api() -> SnapchatAPI:
  return SnapchatAPI(**api_credentials)

@pytest.fixture
def ad_account_id() -> str:
  account_ids = {
    'xyla': 'ACCOUNTID',
  }
  return account_ids['xyla']

def test_pause_campaign(ad_account_id: str, api: SnapchatAPI):
  api.ad_account_id = ad_account_id
  mutator = SnapchatCampaignPauseMutator(
    api=api,
    campaign_id='CAMPAIGNID'
  )
  response = mutator.mutate()
  assert response is not None

def test_mutate_campaign_budget(ad_account_id: str, api: SnapchatAPI):
  api.ad_account_id = ad_account_id
  mutator = SnapchatCampaignBudgetMutator(
    api=api,
    campaign_id='CAMPAIGNID',
    daily_budget_micro=20000000
  )
  response = mutator.mutate()
  assert response is not None