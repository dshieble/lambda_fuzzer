import asyncio
from base64 import b64encode
from typing import Any, Dict

import httpx


async def safe_get(url: str, httpx_client: httpx.AsyncClient, headers: Dict[str, Any]) -> Dict[str, Any]:
  result = {"url": url}
  try:
    result["response"] = await httpx_client.get(
      url,
      follow_redirects=True,
      headers=headers)
  except Exception as e:
    result["error"] = str(e)
  return result


async def async_get_lambda_handler(event, context):

  async with httpx.AsyncClient(verify=False) as httpx_client:
    response_list = await asyncio.gather(*[safe_get(
        url,
        httpx_client=httpx_client,
        headers=event.get('headers', None)
      ) for url in event['url_list']])

  success_response_list, success_url_list = [], []
  error_url_list, error_list = [], []
  for response in response_list:
    if 'response' in response:
      success_response_list.append(response['response'])
      success_url_list.append(response['url'])
    else:
      error_url_list.append(response['url'])
      error_list.append(response['error'])

  return {
    # 'headerList': [dict(response.headers) for response in success_response_list],
    'statusCodeList': [response.status_code for response in success_response_list],
    # 'bodyList': [b64encode(response.read()) for response in response_list],
    'urlList': success_url_list,
    'errorUrlList': error_url_list,
    'errorList': error_list
  }

def lambda_handler(event, context):
  loop = asyncio.get_event_loop()    
  return loop.run_until_complete(async_get_lambda_handler(event, context))  
