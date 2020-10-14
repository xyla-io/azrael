import pytest

from datetime import datetime, timedelta
from ..api import SnapchatAPI
from ..reporting import SnapchatReporter

@pytest.fixture
def reporter() -> SnapchatReporter:
  creds = {
    'refresh_token': 'REFRESHTOKEN',
    'client_id': 'CLIENTID',
    'client_secret': 'CLIENTSECRET',
  }
  api = SnapchatAPI(**creds)
  return SnapchatReporter(api=api)

@pytest.fixture
def ad_account_id() -> str:
  return 'ACCOUNTID'

def test_campaign_stats(reporter: SnapchatReporter, ad_account_id: str):
  columns = [
    'impressions',
    'swipes',
    'view_time_millis',
    'screen_time_millis',
    'quartile_1',
    'quartile_2',
    'quartile_3',
    'view_completion',
    'spend',
    'video_views',
    'frequency',
    'uniques',
    'swipe_up_percent'
  ]
  reporter.api.ad_account_id = ad_account_id
  campaigns = reporter.api.get_campaigns(ad_account_id=ad_account_id)
  campaign = campaigns[0]

  api_start_date = reporter.clamped_date_in_account_timezone(
    date=datetime(2020, 9, 1),
    now=datetime.utcnow()
  )
  api_end_date = reporter.clamped_date_in_account_timezone(
    date=datetime(2020, 9, 2) + timedelta(days=1),
    now=datetime.utcnow()
  )

  df = reporter.get_campaign_stats(
    campaign_id=campaign['id'], 
    start_date=api_start_date,
    end_date=api_end_date, 
    columns=columns,
    swipe_up_attribution_window='7_DAY',
    view_attribution_window='3_HOUR'
  )

  import pdb; pdb.set_trace()
  
  assert df is not None
  assert 'campaign_id' in df.columns

def test_walmart_ad_stats(reporter: SnapchatReporter, ad_account_id: str):
  columns = [
    'impressions',
    'swipes',
    'view_time_millis',
    'screen_time_millis',
    'quartile_1',
    'quartile_2',
    'quartile_3',
    'view_completion',
    'spend',
    'video_views',
    'frequency',
    'uniques',
    'swipe_up_percent'
  ]
  reporter.api.ad_account_id = ad_account_id
  campaigns = reporter.api.get_campaigns(ad_account_id=ad_account_id)
  campaign = campaigns[0]
  df = reporter.get_ad_stats(campaign_id=campaign['id'], start_date=datetime(2018, 10, 20), end_date=datetime(2018, 10, 21), columns=columns)

  import pdb; pdb.set_trace()
  
  assert df is not None
  assert 'ad_id' in df.columns
