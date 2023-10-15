"""
First run
  aws configure --profile danshiebler to set up the AWS credentials
then run
  python test_proxy_manager.py --url_list="https://ipinfo.io/ip,https://fakefake.weeblysite.com,https://electriccitycoffee.weeblysite.com"
"""

import argparse
import asyncio
import json
from base64 import b64decode
import os

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lambda_scraper.proxy_manager import ProxyManager

headers = {
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

async def main(args):
  manager = await ProxyManager()
  url_list = args.url_list.split(",")
  print(f"url_list: {url_list}")
  while True:
    response = await manager.run_lambda_proxy_round_robin(url_list)
    print(response.keys())
    lines = "\n****\n".join([
      f'url: {url} status: {status}'
      for url, status in zip(response["urlList"], response['statusCodeList'])])
    print(f'{lines}')
    print("\n\n\n\n-------------------------\n\n\n\n")


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--url_list", type=str, required=True)
  args = parser.parse_args()
  asyncio.run(main(args))