import pytest

from datetime import datetime
from typing import Dict
from ..api import SnapchatAPI
from ..reporting import SnapchatReporter
from ..io_reporting import IOSnapchatReporter

@pytest.fixture
def credentials() -> Dict[str, any]:
  return {
    'refresh_token': 'REFRESHTOKEN',
    'client_id': 'CLIENTID',
    'client_secret': 'CLIENTSECRET',
  }

@pytest.fixture
def io_reporter(credentials) -> IOSnapchatReporter:
  return None

def test1(credentials):
  io_reporter = IOSnapchatReporter(
    columns=['campaign.name', 'adgroup.name', 'time']
  )
  output = io_reporter.run(credentials=credentials)
  import pdb; pdb.set_trace()