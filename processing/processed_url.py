from collections import defaultdict
from dataclasses import dataclass, field
import io
import json
import time
import httpx
import numpy as np

import PIL.Image
import PIL
from typing import Any, Dict, List, Optional, Tuple, Union
from processing.constants import RecomputationStrategy
from processing.derived_attributes import DerivedAttributes, DerivedAttributesFactory
from processing.phishing_classification_response import PhishingClassificationResponse, PhishingClassificationResponseFactory
from analysis.domain_analysis import  get_domain_records_from_url
from analysis.scraping import create_context_and_get_first_page_screenshot

from utilities.utilities import Jsonable, call_with_rate_limit_retry
from data_tools.tables import URLS
from data_tools.data_access import add_row_dict_to_table
from data_tools.sql_helpers import execute_command_with_fetch
from analysis.scraping import UrlScreenshotResponse







@dataclass
class ProcessedUrl(Jsonable):
  url: str
  experiment_id: str
  timestamp: int
  url_screenshot_response: Optional[UrlScreenshotResponse] = None
  phishing_classification_response: Optional[PhishingClassificationResponse] = None
  derived_attributes: Optional[DerivedAttributes] = None

  @classmethod
  def get_annotation_to_jsonable_class(cls) -> Dict[str, Any]:
    return {
      "url_screenshot_response": UrlScreenshotResponse,
      "phishing_classification_response": PhishingClassificationResponse,
      "derived_attributes": DerivedAttributes
    }

  @classmethod
  async def build_processed_url_from_url(
    cls,
    experiment_id: str,
    url: str,
    phishing_classification_response_factory: PhishingClassificationResponseFactory,
    derived_attributes_factory: DerivedAttributesFactory,
    browser: "BrowserType",
    attribute_recomputation_id: str = None,
    async_callback_fn: "Optional[Callable]" = None
  ) -> "ProcessedUrl":

    # Start by verifying that this domain is discoverable
    domain_records = await get_domain_records_from_url(url=url)

    if len(domain_records) == 0:
      url_screenshot_response = UrlScreenshotResponse(url=url, timestamp=int(time.time()), dns_failure=True)
    else:
      url_screenshot_response = await create_context_and_get_first_page_screenshot(
        browser=browser,
        url=url
      )

    processed_url = ProcessedUrl(
      url=url,
      experiment_id=experiment_id,
      timestamp=url_screenshot_response.timestamp,
      url_screenshot_response=url_screenshot_response,
      phishing_classification_response=None,
      derived_attributes=None, 
    )
    await processed_url.recompute_phishing_classification_response(
      attribute_recomputation_id=attribute_recomputation_id, phishing_classification_response_factory=phishing_classification_response_factory)
    await processed_url.recompute_derived_attributes(
      attribute_recomputation_id=attribute_recomputation_id, derived_attributes_factory=derived_attributes_factory)
    assert processed_url.derived_attributes is not None

    # if write_to_postgres:
    #   await processed_url.write_to_postgres(verbose=True)
    await async_callback_fn(processed_url)
    return processed_url

  @classmethod
  def read_from_postgres_row_dict(cls, row_dict: Dict[str, Any]) -> "ProcessedUrl":
    return cls.from_json_dict(json_dict=row_dict)

  async def recompute_phishing_classification_response(
    self,
    attribute_recomputation_id: str,
    phishing_classification_response_factory: PhishingClassificationResponseFactory
  ):
    # The attribute_recomputation_id stores the computation context for this derived attribute reprocessing
    self.phishing_classification_response = phishing_classification_response_factory.build(
      attribute_recomputation_id=attribute_recomputation_id,
      processed_url=self
    )

  async def recompute_derived_attributes(
    self,
    attribute_recomputation_id: str,
    derived_attributes_factory: DerivedAttributesFactory,
    recomputation_strategy: Optional[RecomputationStrategy] = None
  ):
    # The attribute_recomputation_id stores the computation context for this derived attribute reprocessing
    self.derived_attributes = await derived_attributes_factory.build(
      attribute_recomputation_id=attribute_recomputation_id,
      processed_url=self,
      recomputation_strategy=recomputation_strategy
    )

  async def write_to_postgres(self, verbose: bool = True):
    row_dict = self.to_json_dict(encode_jsonables_as_string=True)
    await add_row_dict_to_table(
      row_dict=row_dict,
      table=URLS
    )
    if verbose:
      print(f"Wrote {self.url} [{self.timestamp}] to postgres")


  def get_screenshot_image(self) -> Optional[PIL.Image.Image]:
    return (None if self.url_screenshot_response is None else self.url_screenshot_response.get_image())

  def display_phishing_classification_response(self):
    if self.phishing_classification_response is None:
      print("ERROR: phishing_classification_response is None")
    else:
      self.phishing_classification_response.display()

  


async def load_processed_url_list_from_sql_query(
  sql_query: str,
  command_vars: Optional[Tuple[Any, ...]],
  params: Optional[Dict[str, str]] = None
) -> "List[ProcessedUrl]":
  """
  Returns a dictionary mapping each url to the sorted list of processed urls, sorted by timestamp
  """
  row_dict_list = await call_with_rate_limit_retry(
    execute_command_with_fetch,
    exception=Exception,
    params=params,
    command=sql_query,
    command_vars=command_vars
  )

  # for row_dict in row_dict_list:
  #   print("row_dict", row_dict)
  processed_url_list = [
    ProcessedUrl.read_from_postgres_row_dict(
      row_dict=row_dict
    )
    for row_dict in row_dict_list
  ]
  
  url_to_processed_url_list = defaultdict(list)
  for processed_url in processed_url_list:
    url_to_processed_url_list[processed_url.url].append(processed_url)
  return {
    url: sorted(processed_url_list, key=lambda processed_url: processed_url.timestamp)
    for url, processed_url_list in url_to_processed_url_list.items()
  }

async def load_url_to_processed_url_list_from_experiment_id(
  experiment_id: str,
  params: Optional[Dict[str, str]] = None
) -> "List[ProcessedUrl]":
  """
  Returns a dictionary mapping each url to the sorted list of processed urls, sorted by timestamp
  """
  sql_query = f"SELECT * from {URLS.name} where experiment_id = %s;"

  return await load_processed_url_list_from_sql_query(
    sql_query=sql_query,
    command_vars=(experiment_id,),
    params=params
  )

async def load_url_to_processed_url_list_from_url_list(
  url_list: List[str],
  params: Optional[Dict[str, str]] = None
) -> "List[ProcessedUrl]":
  """
  Returns a dictionary mapping each url to the sorted list of processed urls, sorted by timestamp
  """
  url_list_filter = ','.join(["%s" for _ in url_list])
  sql_query = f"SELECT * from {URLS.name} where url in ({url_list_filter});"

  return await load_processed_url_list_from_sql_query(
    sql_query=sql_query,
    command_vars=tuple(url_list),
    params=params
  )

async def load_processed_url_list_from_url(url: str) -> "List[ProcessedUrl]":
  """
  Loads a particular url, and then sorts the results by timestamp
  """
  url_to_processed_url_list = await load_url_to_processed_url_list_from_url_list(url_list=[url])
  return sorted(url_to_processed_url_list[url], key=lambda processed_url: processed_url.timestamp)

