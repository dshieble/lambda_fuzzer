from collections import defaultdict
from dataclasses import dataclass, field
import io
import json
import httpx
import numpy as np

import PIL.Image
import PIL
from typing import Any, Dict, List, Optional, Union
import json_numpy
from data_tools.config_manager import ConfigManager
from processing.constants import RecomputationStrategy
from analysis.domain_analysis import DomainClassificationResponse, get_fqdn_from_url
from analysis.domain_lookup import DomainLookupTool, DomainLookupResponse
from analysis.html_understanding import HTMLAttributes

from utilities.utilities import Jsonable


@dataclass
class DerivedAttributes(Jsonable):
  """
  These are the attributes that we extract from the raw data and later use to make predictions
  """

  # The attribute_recomputation_id has a default value of None, but it should always be set when we reprocess derived attributes
  fqdn: str
  url_domain_analysis_response: DomainLookupResponse
  url_domain_classification: DomainClassificationResponse
  final_redirect_fqdn: Optional[str]
  final_redirect_url_domain_analysis_response: DomainLookupResponse
  final_redirect_url_domain_classification: DomainClassificationResponse
  attribute_recomputation_id: Optional[str] = None
  html_attributes: Optional[HTMLAttributes] = None

  @classmethod
  def get_annotation_to_jsonable_class(cls) -> Dict[str, Any]:
    return {
      "url_domain_analysis_response": DomainLookupResponse,
      "url_domain_classification": DomainClassificationResponse,
      "final_redirect_url_domain_analysis_response": DomainLookupResponse,
      "final_redirect_url_domain_classification": DomainClassificationResponse,
      "html_attributes": HTMLAttributes,
    }

@dataclass
class DerivedAttributesFactory:
  """
  This class is used to create DerivedAttributes objects from a ProcessedUrl object
  """
  domain_lookup_tool: DomainLookupTool
  httpx_client: httpx.AsyncClient
  config_manager: ConfigManager
  verbose: bool = True

  async def build(
    self,
    processed_url: "ProcessedUrl",
    attribute_recomputation_id: Optional[str] = None,
    recomputation_strategy: Optional[RecomputationStrategy] = None,
    verbose: bool = True
  ) -> DerivedAttributes:
    
    # Recompute light stuff
    fqdn = get_fqdn_from_url(url=processed_url.url)
    url_domain_classification= DomainClassificationResponse.from_fqdn(fqdn=fqdn, config_manager=self.config_manager)

    if processed_url.url_screenshot_response is not None and processed_url.url_screenshot_response.final_redirect_url is not None:
      final_redirect_fqdn = get_fqdn_from_url(url=processed_url.url_screenshot_response.final_redirect_url)
      final_redirect_url_domain_classification = DomainClassificationResponse.from_fqdn(fqdn=final_redirect_fqdn, config_manager=self.config_manager)
    else:
      final_redirect_fqdn = None
      final_redirect_url_domain_classification = None

    html_attributes = (
      None if processed_url.url_screenshot_response is None or processed_url.url_screenshot_response.html is None 
      else HTMLAttributes.from_html(html=processed_url.url_screenshot_response.html)
    )

    # Recompute heavy stuff
    if recomputation_strategy in [RecomputationStrategy.RECOMPUTE_LIGHT_DERIVED_ATTRIBUTES]:
      # Use the existing values if we aren't recomputing everything
      url_domain_analysis_response = (
        None if processed_url.derived_attributes is None else processed_url.derived_attributes.url_domain_analysis_response
      )
      final_redirect_url_domain_analysis_response = (
        None if processed_url.derived_attributes is None else processed_url.derived_attributes.final_redirect_url_domain_analysis_response
      )
    else:
      # The default behavior when recomputation_strategy is None is to recompute everything
      url_domain_analysis_response = await DomainLookupResponse.from_fqdn(
        fqdn=fqdn, domain_lookup_tool=self.domain_lookup_tool, httpx_client=self.httpx_client, verbose=verbose)

      if processed_url.url_screenshot_response is not None and processed_url.url_screenshot_response.final_redirect_url is not None:
        final_redirect_url_domain_analysis_response = await DomainLookupResponse.from_fqdn(
          fqdn=final_redirect_fqdn, domain_lookup_tool=self.domain_lookup_tool, httpx_client=self.httpx_client, verbose=verbose)
      else:
        final_redirect_url_domain_analysis_response = None



    return DerivedAttributes(
      attribute_recomputation_id=attribute_recomputation_id,
      fqdn=fqdn,
      html_attributes=html_attributes,
      url_domain_analysis_response=url_domain_analysis_response,
      url_domain_classification=url_domain_classification,
      final_redirect_fqdn=final_redirect_fqdn,
      final_redirect_url_domain_analysis_response=final_redirect_url_domain_analysis_response,
      final_redirect_url_domain_classification=final_redirect_url_domain_classification
    )
