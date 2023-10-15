
import time
from typing import List, Optional

import httpx
from data_tools.config_manager import ConfigManager
from data_tools.data_access import update_postgres_table
from data_tools.tables import URLS
from phishpedia.phishpedia_main import PhishpediaClassifier
from processing.constants import RecomputationStrategy
from processing.derived_attributes import DerivedAttributesFactory
from processing.phishing_classification_response import PhishingClassificationResponseFactory
from processing.processed_url import ProcessedUrl, load_processed_url_list_from_url, load_url_to_processed_url_list_from_experiment_id
from analysis.domain_lookup import DomainLookupTool

from playwright.async_api import async_playwright

from utilities.utilities import chunked_gather



async def recompute_attributes_and_update_postgres(
  processed_url: ProcessedUrl,
  attribute_recomputation_id: str,
  recomputation_strategy: RecomputationStrategy,
  derived_attributes_factory: DerivedAttributesFactory,
  verbose: bool = True,
  phishing_classification_response_factory: Optional[PhishingClassificationResponseFactory] = None
) -> ProcessedUrl:
  if recomputation_strategy == RecomputationStrategy.RECOMPUTE_PHISHING_CLASSIFICATION_RESPONSE_AND_DERIVED_ATTRIBUTES:
    assert phishing_classification_response_factory is not None
    await processed_url.recompute_phishing_classification_response(
      attribute_recomputation_id=attribute_recomputation_id,
      phishing_classification_response_factory=phishing_classification_response_factory)
    await processed_url.recompute_derived_attributes(
      attribute_recomputation_id=attribute_recomputation_id, derived_attributes_factory=derived_attributes_factory, recomputation_strategy=recomputation_strategy)
    
    set_key_to_value = {
      "derived_attributes_json": processed_url.derived_attributes.to_json_string(),
      "phishing_classification_response_json": processed_url.phishing_classification_response.to_json_string()
    }
  elif recomputation_strategy in [RecomputationStrategy.RECOMPUTE_LIGHT_DERIVED_ATTRIBUTES, RecomputationStrategy.RECOMPUTE_DERIVED_ATTRIBUTES]:
    await processed_url.recompute_derived_attributes(
      attribute_recomputation_id=attribute_recomputation_id, derived_attributes_factory=derived_attributes_factory, recomputation_strategy=recomputation_strategy)
    set_key_to_value = {
      "derived_attributes_json": processed_url.derived_attributes.to_json_string()
    }
  else:
    raise ValueError(f"Unrecognized recomputation strategy {recomputation_strategy}")
  
  await update_postgres_table(
    set_key_to_value=set_key_to_value,
    condition_key_to_value={
      "url": processed_url.url,
      "experiment_id": processed_url.experiment_id,
      "timestamp": processed_url.timestamp
    },
    table=URLS)
  if verbose:
    print(f"Updated {processed_url.url} [{processed_url.timestamp}] [{processed_url.experiment_id}] in postgres")
  return processed_url


async def process_and_write_url(
  url: str,
  experiment_id: Optional[str] = "test",
  **kwargs
) -> List[ProcessedUrl]:
  return (await process_and_write_url_list(
    url_list=[url],
    experiment_id=experiment_id,
    **kwargs
  ))[0]
  
async def process_and_write_url_list(
  *args,
  **kwargs
) -> List[ProcessedUrl]:
  """
  Call process_url_list with a callback to write to postgres
  """
  async def async_callback_fn(processed_url: ProcessedUrl):
    await processed_url.write_to_postgres(verbose=True)
  return await process_url_list(*args, **kwargs, async_callback_fn=async_callback_fn)


async def process_url(
  url: str,
  experiment_id: Optional[str] = "test",
  **kwargs
) -> List[ProcessedUrl]:
  return (await process_url_list(
    url_list=[url],
    experiment_id=experiment_id,
    **kwargs
  ))[0]

async def process_url_list(
  url_list: List[str],
  experiment_id: str,
  chunk_size: int = 100,
  verbose: bool = True,
  async_callback_fn: "Optional[Callable]" = None,
  attribute_recomputation_id: str = None
) -> List[ProcessedUrl]:
  domain_lookup_tool = DomainLookupTool()
  config_manager = ConfigManager()
  phishing_classification_response_factory = PhishingClassificationResponseFactory(
    clf=PhishpediaClassifier()
  )
  async with async_playwright() as playwright_context_manager:
    async with httpx.AsyncClient(verify=False) as httpx_client:
      derived_attributes_factory = DerivedAttributesFactory(
        domain_lookup_tool=domain_lookup_tool,
        httpx_client=httpx_client,
        config_manager=config_manager
      )

      browser = await playwright_context_manager.chromium.launch(headless=True)
      processed_url_list = await chunked_gather(
        awaitable_list=[
          ProcessedUrl.build_processed_url_from_url(
            url=url,
            browser=browser,
            experiment_id=experiment_id,
            phishing_classification_response_factory=phishing_classification_response_factory,
            derived_attributes_factory=derived_attributes_factory,
            attribute_recomputation_id=attribute_recomputation_id,
            async_callback_fn=async_callback_fn
          ) for url in url_list
        ],
        chunk_size=chunk_size,
        verbose=verbose
      )
  return processed_url_list
      

async def recompute_attributes_for_processed_url_list(
  processed_url_list: List[ProcessedUrl],
  attribute_recomputation_id: str,
  recomputation_strategy: RecomputationStrategy,
  chunk_size: int = 100,
  verbose: bool = True
) -> List[ProcessedUrl]:
  # Given an experiment_id, pull each url in that experiment id, recompute the derived attributes, and update the postgres table
  domain_lookup_tool = DomainLookupTool()
  config_manager = ConfigManager()
  
  
  phishing_classification_response_factory = PhishingClassificationResponseFactory(
    clf=PhishpediaClassifier()
  )

  filtered_processed_url_list = [p for p in processed_url_list if p.derived_attributes is None or p.derived_attributes.attribute_recomputation_id != attribute_recomputation_id]
  print(f"Updating {len(filtered_processed_url_list)} urls out of {len(processed_url_list)}")
  async with httpx.AsyncClient(verify=False) as httpx_client:
    derived_attributes_factory = DerivedAttributesFactory(
      domain_lookup_tool=domain_lookup_tool,
      httpx_client=httpx_client,
      config_manager=config_manager
    )

    return await chunked_gather(
      awaitable_list=[
        recompute_attributes_and_update_postgres(
          processed_url=processed_url,
          recomputation_strategy=recomputation_strategy,
          attribute_recomputation_id=attribute_recomputation_id,
          derived_attributes_factory=derived_attributes_factory,
          phishing_classification_response_factory=phishing_classification_response_factory
        )
        for processed_url in processed_url_list
      ],
      chunk_size=chunk_size,
      verbose=verbose
    )

async def recompute_attributes_for_url(
  url: str,
  attribute_recomputation_id: str,
  recomputation_strategy: RecomputationStrategy,
  chunk_size: int = 100,
  verbose: bool = True
):
  # Pull each record matching the url, recompute the derived attributes, and update the postgres table

  processed_url_list = load_processed_url_list_from_url(url=url)
  await recompute_attributes_for_processed_url_list(
    processed_url_list=processed_url_list,
    attribute_recomputation_id=attribute_recomputation_id,
    recomputation_strategy=recomputation_strategy,
    chunk_size=chunk_size,
    verbose=verbose)


async def recompute_attributes_for_experiment_id(
  experiment_id: str,
  attribute_recomputation_id: str,
  recomputation_strategy: RecomputationStrategy,
  chunk_size: int = 100,
  verbose: bool = True
):
  # Given an experiment_id, pull each url in that experiment id, recompute the derived attributes, and update the postgres table
  
  url_to_processed_url_list = await load_url_to_processed_url_list_from_experiment_id(experiment_id=experiment_id)
  processed_url_list = []
  for _processed_url_list in url_to_processed_url_list.values():
    processed_url_list += _processed_url_list
  await recompute_attributes_for_processed_url_list(
    processed_url_list=processed_url_list,
    attribute_recomputation_id=attribute_recomputation_id,
    recomputation_strategy=recomputation_strategy,
    chunk_size=chunk_size,
    verbose=verbose)
      
