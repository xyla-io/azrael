import json

from .api import SnapchatAPI
from typing import Optional, Dict

class SnapchatMutator:
  api: SnapchatAPI

  def __init__(self, api: SnapchatAPI):
    self.api = api

  def mutate(self) -> any:
    raise NotImplementedError()

  def prepare_mutation(self, mutation: any):
    raise NotImplementedError()
  
  def _assert_success(self, response):
    assert response['request_status'] == 'SUCCESS'

class SnapchatCampaignMutator(SnapchatMutator):
  campaign_id: str

  def __init__(self, api: SnapchatAPI, campaign_id: str):
    self.campaign_id = campaign_id
    super().__init__(api=api)
  
  @property
  def endpoint(self) -> str:
    return f'adaccounts/{self.api.ad_account_id}/campaigns'
  
  def get_campaign(self) -> Dict[str, any]:
    return self.api.get_campaign(campaign_id=self.campaign_id)

  def mutate(self) -> any:
    campaign = self.get_campaign()
    self.prepare_mutation(campaign)

    payload = {'campaigns': [campaign]}
    response = self.api.put(
      endpoint=self.endpoint,
      data=payload
    ).json()

    self._assert_success(response)
    return response

class SnapchatCampaignPauseMutator(SnapchatCampaignMutator):
  def prepare_mutation(self, mutation: any):
    mutation['status'] = 'PAUSED'

class SnapchatCampaignBudgetMutator(SnapchatCampaignMutator):
  daily_budget_micro: Optional[int]=None

  def __init__(self, api: SnapchatAPI, campaign_id: str, daily_budget_micro: Optional[int]=None):
    self.daily_budget_micro = daily_budget_micro
    super().__init__(api=api, campaign_id=campaign_id)

  def prepare_mutation(self, mutation: any):
    # Notes on daily budget:
    # * the minimum valid daily budget is $20
    # * a daily budget can be removed by setting this key to None
    mutation['daily_budget_micro'] = self.daily_budget_micro