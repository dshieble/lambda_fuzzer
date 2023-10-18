import json
from base64 import b64decode
import os
from typing import Any, Dict, List

import aioboto3, botocore

HEADERS = {
  "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
  "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
  "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"macOS"',
  "sec-fetch-dest": "document",
  "sec-fetch-mode": "navigate",
  "sec-fetch-site": "none",
  "sec-fetch-user": "?1",
  "upgrade-insecure-requests": "1",
}

class ProxyManager:

  def __init__(
    self,
    aws_profile_name: str
  ):
    self.session = aioboto3.Session(
      region_name='us-east-1',
      profile_name=aws_profile_name
      
    )
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    self.round_robin = 0


  async def run_lambda_proxy(self, url_list: List[str], proxy_number: int) -> Dict[str, Any]:
    config = botocore.config.Config(connect_timeout=600, read_timeout=600)
    async with self.session.client(
      "lambda",
      config=config
    ) as lambda_client:
      raw_result = await lambda_client.invoke(
        FunctionName=f"proxy-{proxy_number}",
        InvocationType="RequestResponse",
        Payload=json.dumps({"method": "GET", "url_list": url_list, "headers": HEADERS}),
      )
      payload = await raw_result["Payload"].read()
    response = json.loads(payload)
    if "bodyList" in response:
      response["bodyList"] = [b64decode(b) for b in response["bodyList"]]
    return response
