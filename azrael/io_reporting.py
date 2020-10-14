import pandas as pd
from io_channel import IOChannelGranularity, IOChannelProperty, IOTimeGranularity, IOEntityGranularity, IOTimeMetric, IOEntityMetric, IOEntityAttribute, IOReportOption

from .api import SnapchatAPI
from .reporting import SnapchatReporter
from datetime import datetime, timedelta
from io_channel import IOChannelSourceReporter
from typing import List, Dict, Optional
from math import ceil

class IOSnapchatReporter(IOChannelSourceReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return f'azrael/{cls.__name__}'

  @property
  def start_date(self) -> Optional[datetime]:
    date = self.get_from_filters('start_date')
    return datetime.strptime(date, '%Y-%m-%d') if date else None
  
  @property
  def end_date(self) -> Optional[datetime]:
    date = self.get_from_filters('end_date')
    return datetime.strptime(date, '%Y-%m-%d') if date else None

  @property
  def time_granularity(self) -> Optional[IOTimeGranularity]:
    raw_granularity = self.get_from_options(IOReportOption.time_granularity)
    return IOTimeGranularity(raw_granularity) if raw_granularity else None

  def io_time_granularity_to_api(self, granularity: IOTimeGranularity) -> Optional[str]:
    if granularity is IOTimeGranularity.hourly:
      return 'HOUR'
    elif granularity is IOTimeGranularity.daily:
      return 'DAY'
    else:
      return None

  def io_time_metric_to_api(self, metric: IOTimeMetric, granularity: Optional[IOTimeGranularity]=None) -> Optional[str]:
    if metric is IOTimeMetric.time:
      return 'start_time'
    else:
      return None

  def io_entity_granularity_to_api(self, granularity: IOEntityGranularity) -> Optional[str]:
    if granularity is IOEntityGranularity.adgroup:
      return 'adsquad'
    else:
      return super().io_entity_granularity_to_api(granularity)

  def io_entity_attribute_to_api(self, attribute: IOEntityAttribute, granularity: IOEntityGranularity) -> Optional[str]:
    if attribute is IOEntityAttribute.id and granularity:
      api_granularity = self.io_entity_granularity_to_api(granularity)
      if api_granularity:
        return f'{api_granularity}_id'
      else:
        return None
    return super().io_entity_attribute_to_api(
      attribute=attribute,
      granularity=granularity
    )

  def api_column_to_io(self, api_report: pd.DataFrame, api_column: str, granularity: IOChannelGranularity, property: IOChannelProperty) -> Optional[any]:
    if api_column not in api_report:
      return None
    if property is IOEntityMetric.spend:
      return api_report[api_column] / 1000000
    if property is IOTimeMetric.time:
      return pd.to_datetime(api_report[api_column]).dt.tz_convert(tz='UTC').dt.tz_localize(tz=None)
    return super().api_column_to_io(
      api_report=api_report,
      api_column=api_column,
      granularity=granularity,
      property=property
    )

  def fetch_entity_metrics_report(self, reporter: SnapchatReporter, api_time_granularity: str, api_entity_granularity: str, api_columns: List[str], entity_ids: Optional[List[str]]=None):
    assert self.start_date is not None and self.end_date is not None

    # TODO: Support daily granulairity if necessary by converting dates to the account's time zone
    assert api_time_granularity == 'HOUR', 'Only hourly granularity is currently supported'
    start = self.start_date
    end = self.end_date + timedelta(days=1)

    # Snapchat only supports a 7 day interval when retriving hourly stats
    report_interval = timedelta(seconds=60 * 60 * 24 * 7)
    periods = ceil((end - start) / report_interval)
    report = None

    for period in range(periods):
      period_start = start + report_interval * period
      period_end = min(period_start + report_interval, end)
      period_report = reporter.get_performance_report(
        time_granularity=api_time_granularity,
        entity_granularity=api_entity_granularity,
        entity_ids=entity_ids,
        start_date=period_start,
        end_date=period_end,
        columns=api_columns
      )
      if report is None:
        report = period_report
      else:
        report = report.append(period_report)

    report.reset_index(drop=True, inplace=True)
    return report

  def fetch_entity_report(self, granularity: IOEntityGranularity, reporter: SnapchatReporter) -> pd.DataFrame:
    api_entity_columns = self.filtered_api_entity_attributes(granularity=granularity)
    api_higher_entity_identifiers = [
      self.io_to_api(g.identifier_property.value, g.value)
      for g in granularity.higher
      if g <= IOEntityGranularity.campaign
    ]
    api_entity_columns.extend([i for i in api_higher_entity_identifiers if i not in api_entity_columns])
    api_entity_granularity = self.io_to_api(granularity.value)
    api_entity_id_column = f'{api_entity_granularity}_id'
    api_entity_columns = list(filter(lambda c: c != api_entity_id_column, api_entity_columns))

    if granularity is IOEntityGranularity.account:
      filtered_account = {
        k: v
        for k, v in reporter.api.get_ad_account().items()
        if k in api_entity_columns
      }
      return pd.DataFrame([filtered_account])

    api_time_granularity = self.io_to_api(self.time_granularity.value if self.time_granularity else IOTimeGranularity.daily.value)
    report = pd.DataFrame()
    # TODO: Extract entity IDs from filters
    entity_ids = None
    if api_entity_columns or entity_ids is None:
      attribute_report = reporter.get_performance_report(
        time_granularity=api_time_granularity,
        entity_granularity=api_entity_granularity,
        entity_ids=entity_ids,
        entity_columns=api_entity_columns
      )
      if not attribute_report.empty:
        entity_ids = sorted(attribute_report[f'{api_entity_granularity}_id'].unique())
        report = report.append(attribute_report)

    api_metric_columns = self.filtered_api_entity_metrics(granularity=granularity)
    if api_metric_columns:
      metric_report = self.fetch_entity_metrics_report(
        reporter=reporter,
        api_time_granularity=api_time_granularity,
        api_entity_granularity=api_entity_granularity,
        api_columns=api_metric_columns,
        entity_ids=entity_ids
      )
      if api_higher_entity_identifiers and not attribute_report.empty and not metric_report.empty:
        metric_report = metric_report.merge(
          attribute_report[[
            *api_higher_entity_identifiers,
            api_entity_id_column,
          ]],
          how='left',
          on=api_entity_id_column,
          suffixes=('', '')
        )
      report = report.append(metric_report)

    report.reset_index(drop=True, inplace=True)
    report[self.io_entity_attribute_to_api(IOEntityGranularity.account.identifier_property, IOEntityGranularity.account)] = reporter.api.ad_account_id
    return report

  def run(self, credentials: Dict[str, any]) -> Dict[str, any]:
    api = SnapchatAPI(**credentials)
    reporter = SnapchatReporter(api=api)
    report = pd.DataFrame()
    
    for granularity in self.filtered_io_entity_granularities:
      api_report = self.fetch_entity_report(granularity=granularity, reporter=reporter)
      io_report = self.api_report_to_io(
        api_report=api_report,
        granularities=[
          granularity,
          *([self.time_granularity] if self.time_granularity else []),
        ])
      self.fill_api_ancestor_identifiers_in_io(
        api_report=api_report,
        io_report=io_report,
        granularities=[
          granularity,
          *([self.time_granularity] if self.time_granularity else []),
        ]
      )
      report = report.append(io_report)

    report = self.finalized_io_report(report)
    return report
