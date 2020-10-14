import re
import maya
import pandas as pd

from .api import SnapchatAPI
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from enum import Enum
from math import ceil

class Entity(Enum):
  ad = 'ad'
  adsquad = 'adsquad'
  campaign = 'campaign'

class SnapchatReporter:
  api: SnapchatAPI

  def __init__(self, api: SnapchatAPI):
    self.api = api

  def iso_date(self, date: datetime) -> str:
    return maya.when(date.isoformat()).iso8601()
  
  def clamped_date_in_account_timezone(self, date: datetime, now: datetime) -> datetime:
    converted_time = maya.when(date.strftime('%Y-%m-%d'), timezone=self.api.ad_account['timezone']).datetime()
    last_valid_time = now - timedelta(days=-1)
    time_until_valid = maya.when(last_valid_time.isoformat()).datetime() - converted_time

    valid_time = datetime(converted_time.year, converted_time.month, converted_time.day, converted_time.hour)
    total_seconds_until_valid = time_until_valid.total_seconds() + (time_until_valid.microseconds / 1000000)

    if total_seconds_until_valid < 0:
      days = ceil(-total_seconds_until_valid / 60 / 60 / 24)
      valid_converted_time = maya.when((date - timedelta(days=days)).strftime('%Y-%m-%d'), timezone=self.api.ad_account['timezone']).datetime()
      valid_time = datetime(valid_converted_time.year, valid_converted_time.month, valid_converted_time.day, valid_converted_time.hour)

    return valid_time

  def reformatted_response_date(self, response_date: str) -> str:
    return re.sub(r'((\+|-)\d\d):(\d\d)$', '000\\1\\3', response_date)

  def data_frame_from_response(self, response: Dict[str, any], columns: List[str], entity: Entity):
    if entity is Entity.campaign:
      rows = [
        {
          'campaign_id': r['timeseries_stat']['id'],
          'start_time': t['start_time'],
          'end_time': t['end_time'],
          **t['stats'],
        }
        for r in response['timeseries_stats']
        for t in r['timeseries_stat']['timeseries']
      ]
    else:
      rows = [
        {
          entity.value + '_id': e['id'],
          'start_time': t['start_time'],
          'end_time': t['end_time'],
          **t['stats'],
        }
        for r in response['timeseries_stats']
        for e in r['timeseries_stat']['breakdown_stats'][entity.value]
        for t in e['timeseries']
      ]

    df =  pd.DataFrame(rows)
    if df.empty:
      return df

    df.start_time = df.start_time.apply(self.reformatted_response_date)
    df.end_time = df.end_time.apply(self.reformatted_response_date)
    return df
  
  def get_ad_stats(self, campaign_id: str, start_date: datetime, end_date: datetime, columns: List[str], swipe_up_attribution_window: Optional[str]=None, view_attribution_window: Optional[str]=None) -> pd.DataFrame:
    endpoint = 'campaigns/{campaign_id}/stats'.format(campaign_id=campaign_id)
    parameters = {
      'breakdown': 'ad',
      'granularity': 'DAY',
      'fields': ','.join(columns),
      'start_time': self.iso_date(start_date),
      'end_time': self.iso_date(end_date),
      **({'swipe_up_attribution_window': swipe_up_attribution_window} if swipe_up_attribution_window else {}),
      **({'view_attribution_window': view_attribution_window} if view_attribution_window else {}),
    }

    response = self.api.get(endpoint=endpoint, parameters=parameters).json()
    df = self.data_frame_from_response(response=response, columns=columns, entity=Entity.ad)
    df['campaign_id'] = campaign_id
    return df
  
  def get_adsquad_stats(self, campaign_id: str, start_date: datetime, end_date: datetime, columns: List[str], swipe_up_attribution_window: Optional[str]=None, view_attribution_window: Optional[str]=None) -> pd.DataFrame:
    endpoint = 'campaigns/{campaign_id}/stats'.format(campaign_id=campaign_id)
    parameters = {
      'breakdown': 'adsquad',
      'granularity': 'DAY',
      'fields': ','.join(columns),
      'start_time': self.iso_date(start_date),
      'end_time': self.iso_date(end_date),
      **({'swipe_up_attribution_window': swipe_up_attribution_window} if swipe_up_attribution_window else {}),
      **({'view_attribution_window': view_attribution_window} if view_attribution_window else {}),
    }

    response = self.api.get(endpoint=endpoint, parameters=parameters).json()
    df = self.data_frame_from_response(response=response, columns=columns, entity=Entity.adsquad)
    df['campaign_id'] = campaign_id
    return df

  def get_campaign_stats(self, campaign_id: str, start_date: datetime, end_date: datetime, columns: List[str], swipe_up_attribution_window: Optional[str]=None, view_attribution_window: Optional[str]=None) -> pd.DataFrame:
    endpoint = 'campaigns/{campaign_id}/stats'.format(campaign_id=campaign_id)
    parameters = {
      'granularity': 'DAY',
      'fields': ','.join(columns),
      'start_time': self.iso_date(start_date),
      'end_time': self.iso_date(end_date),
      **({'swipe_up_attribution_window': swipe_up_attribution_window} if swipe_up_attribution_window else {}),
      **({'view_attribution_window': view_attribution_window} if view_attribution_window else {}),
    }

    response = self.api.get(endpoint=endpoint, parameters=parameters).json()
    df = self.data_frame_from_response(response=response, columns=columns, entity=Entity.campaign)
    df['campaign_id'] = campaign_id
    return df
  
  def get_performance_report(self, time_granularity: str, entity_granularity: str, entity_ids: Optional[List[str]]=None, columns: List[str]=[], entity_columns: List[str]=[], start_date: Optional[datetime]=None, end_date: Optional[datetime]=None) -> pd.DataFrame:
    if entity_ids is None:
      entity_ids = []
    elif len(entity_ids) == 0:
      return pd.DataFrame()

    entity_type = Entity(entity_granularity)
    if entity_columns or len(entity_ids) == 0:
      if entity_type is Entity.campaign:      
        entities = [self.api.get_campaign(campaign_id=entity_ids[0])] if len(entity_ids) == 1 else self.api.get_campaigns()
      elif entity_type is Entity.adsquad:
        entities = [self.api.get_adsquad(adsquad_id=entity_ids[0])] if len(entity_ids) == 1 else self.api.get_adsquads()
      elif entity_type is Entity.ad:
        entities = [self.api.get_ad(ad_id=entity_ids[0])] if len(entity_ids) == 1 else self.api.get_ads()
      else:
        raise ValueError('Unsupported entity type', entity_type)

      if len(entity_ids) == 0:
        entity_ids = sorted([e['id'] for e in entities])
    else:
      entities = []

    parameters = {
      'granularity': time_granularity,
      'fields': ','.join(columns),
      'start_time': self.iso_date(start_date),
      'end_time': self.iso_date(end_date),
    } if columns else {}

    all_rows = []
    for entity_id in entity_ids:
      if columns:
        response = self.api.get(
          endpoint=f'{entity_type.value}s/{entity_id}/stats',
          parameters=parameters
        ).json()
        rows = [
          {
            f'{entity_type.value}_id': entity_id,
            'start_time': t['start_time'],
            'end_time': t['end_time'],
            **t['stats'],
          }
          for r in response['timeseries_stats']
          for t in r['timeseries_stat']['timeseries']
        ]
      else:
        rows = [
          {
            f'{entity_type.value}_id': entity_id,
          },
        ]
      
      if entity_columns:
        matching_entities = list(filter(lambda e: e['id'] == entity_id, entities))
        assert len(matching_entities) == 1, f'Entity ID {entity_id} does not match exactly on entity {matching_entities}'
        entity = matching_entities[0]
        rows = [
          {
            **r,
            **{
              c: entity[c]
              for c in entity_columns
            },
          }
          for r in rows
        ]

      all_rows.extend(rows)

    df =  pd.DataFrame(all_rows)
    if df.empty:
      return df

    if columns:
      df.start_time = df.start_time.apply(self.reformatted_response_date)
      df.end_time = df.end_time.apply(self.reformatted_response_date)
    return df