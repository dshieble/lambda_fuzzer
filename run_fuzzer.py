import argparse
import asyncio
import numpy as np
from typing import Dict, List, Optional
import logging
import os
import uuid
import pybloomfilter
import aioboto3
from dataclasses import dataclass 

from lambda_fuzzer.proxy_manager import ProxyManager

# We add urls that have already been looked at to this bloom filter to avoid reprocessing them
BLOOM_FILTER_SIZE = 10000000000
BLOOM_FILTER_ERROR = 0.001


def load_bloom_filter(bloom_filter_path: Optional[str]) -> pybloomfilter.BloomFilter:
  """
  Load bloom filter from the bloom_filter_path if it exists, otherwise create a new one
  """
  bloom_filter_path = bloom_filter_path if bloom_filter_path is not None else f"/tmp/{uuid.uuid4()}.bloom"
  logging.info(f"Using bloom_filter_path: {bloom_filter_path}")
  if os.path.exists(bloom_filter_path):
    bloom_filter = pybloomfilter.BloomFilter.open(bloom_filter_path)
  else:
    bloom_filter = pybloomfilter.BloomFilter(BLOOM_FILTER_SIZE, BLOOM_FILTER_ERROR, bloom_filter_path)
  return bloom_filter

@dataclass
class UrlWriter:
  s3_bucket_name: str
  s3_directory_key: str
  bloom_filter: pybloomfilter.BloomFilter
  session: aioboto3.Session

  @classmethod
  async def create_url_writer(
    cls,
    s3_bucket_name: str,
    s3_directory_key: str,
    aws_profile_name: str,
    bloom_filter_path: str,
    aws_region_name: str
  ):
    os.environ['AWS_DEFAULT_REGION'] = aws_region_name
    session = aioboto3.Session(
      region_name=aws_region_name,
      profile_name=aws_profile_name
    )
    
    async with session.client('s3') as s3_client:
      # Check whether the bucket exists and create it if it does not
      try:
        await s3_client.head_bucket(Bucket=s3_bucket_name)
      except Exception as e:
        logging.error(f"S3 Bucket {s3_bucket_name} not found! Creating the bucket.")
        await s3_client.create_bucket(Bucket=s3_bucket_name)
    return cls(
      s3_bucket_name=s3_bucket_name,
      s3_directory_key=s3_directory_key,
      bloom_filter=load_bloom_filter(bloom_filter_path=bloom_filter_path),
      session=session
    )

  async def write_url_list_to_s3(self, url_list: List[str]):
    if len(url_list) > 0:
      url_list_string = "\n".join(url_list) + "\n"
      key = os.path.join(self.s3_directory_key, str(uuid.uuid4()))
      async with self.session.client('s3') as s3_client:
        await s3_client.put_object(Body=url_list_string, Bucket=self.s3_bucket_name, Key=key)


  def write_url_list_to_bloom(self, url_list: List[str]):
    for url in url_list:
      self.bloom_filter.update((url,))

  def get_filtered_urls(self, url_list: List[str]) -> List[str]:
    return [url for url in url_list if url not in self.bloom_filter]


  async def get_urls_to_scan_from_fuzz_terms_file(self, path_to_fuzz_terms_file: str) -> List[str]:
    with open(path_to_fuzz_terms_file, 'r') as f:
      fuzz_term_list = f.readlines()
    full_url_list = [args.url_template % fuzz_term.strip() for fuzz_term in fuzz_term_list]
    
    url_list = self.get_filtered_urls(url_list=full_url_list)
    print(f"Fuzzing {len(url_list)} urls out of {len(full_url_list)} total urls.")
    
    example_urls = '\n'.join(url_list[:10])
    print(f"Example Urls: {example_urls}")
    return url_list



async def execute_proxy_discovery_on_url_list(
  url_list: List[str],
  proxy_manager: ProxyManager,
  proxy_number: int,
  url_writer: UrlWriter
):
  # We use a random sleep here to avoid all of the proxies being called at the same time
  await asyncio.sleep(10 * float(np.random.random()))

  # First we check which urls are active using an AWS lambda proxy
  try:
    response = await proxy_manager.run_lambda_proxy(url_list=url_list, proxy_number=proxy_number)
  except Exception as e:
    print(f"ERROR on proxy number: {proxy_number}")
    print(e)
  else:
    if "urlList" not in response:
      # TODO: Add retry logic
      logging.error(
        f"""
        FAILED TO GET RESPONSE FROM LAMBDA PROXY {proxy_number}
        response: {response}
        url_list: {url_list}
        """)
    else:
      assert len(set(response["errorUrlList"]).intersection(set(response["urlList"]))) == 0
      # Next we check the status codes and add urls with non-success status codes to the bloom filter
      success_code_urls = [
        url for url, status_code in zip(response["urlList"], response["statusCodeList"])
        if str(status_code).startswith('2')
      ]

      logging.info(f"Writing {len(success_code_urls)} success code urls to s3 and {len(url_list)} total urls to bloom filter")
      await url_writer.write_url_list_to_s3(url_list=success_code_urls)
      url_writer.write_url_list_to_bloom(url_list=url_list)



async def main(args):

  url_writer = await UrlWriter.create_url_writer(
    s3_bucket_name=args.s3_bucket_name,
    s3_directory_key=args.s3_directory_key,
    aws_profile_name=args.aws_profile_name,
    aws_region_name=args.aws_region_name,
    bloom_filter_path=args.bloom_filter_path)

  full_url_list = await url_writer.get_urls_to_scan_from_fuzz_terms_file(path_to_fuzz_terms_file=args.path_to_fuzz_terms_file)

  # Split the url_list into chunks, each of which will be passed to a single proxy at a time
  chunked_url_list = []
  for i in range(0, len(full_url_list), args.number_urls_to_process_per_proxy_call):
    chunked_url_list.append(full_url_list[i:i+args.number_urls_to_process_per_proxy_call])

  # Iterate through groups of num_proxies chunks at a time
  num_proxies = int(args.max_proxy) - int(args.min_proxy)
  for i in range(0, len(chunked_url_list), num_proxies):
    print(f"PROCESSING CHUNK: {i}:{min(len(chunked_url_list), i+num_proxies)} out of {len(chunked_url_list)}")
    proxy_manager = ProxyManager(aws_profile_name=args.aws_profile_name)

    await asyncio.gather(*[
      execute_proxy_discovery_on_url_list(
        url_list=url_list,
        proxy_manager=proxy_manager,
        proxy_number=proxy_number,
        url_writer=url_writer
      )
      for proxy_number, url_list in zip(range(int(args.min_proxy), int(args.max_proxy)), chunked_url_list[i:i+num_proxies])
    ])


if __name__ == "__main__":
  """

  python run_fuzzer.py \
    --path_to_fuzz_terms_file=<path to the text file of fuzzing terms> \
    --aws_profile_name=<the name of the aws profile to use> \
    --url_template=<the template of the url to use, with `%s` in place of the term to fuzz> \
    --s3_bucket_name=<the bucket on s3 to write the text files of urls that returned 200 codes> 
    --s3_directory_key=<the key within the s3 bucket to write to> 

  """
  parser = argparse.ArgumentParser()
  parser.add_argument("--url_template", type=str, required=True) 
  parser.add_argument("--path_to_fuzz_terms_file", type=str, required=True)
  parser.add_argument("--s3_bucket_name", type=str, required=True)
  parser.add_argument("--s3_directory_key", type=str, required=True)

  parser.add_argument("--aws_profile_name", type=str, required=True)

  parser.add_argument("--bloom_filter_path", type=str, default=None) 
  parser.add_argument("--number_urls_to_process_per_proxy_call", type=int, default=100)
  parser.add_argument("--aws_region_name", type=str, default="us-east-1")
  parser.add_argument("--min_proxy", type=int, default=0)
  parser.add_argument("--max_proxy", type=int, default=10)

  args = parser.parse_args()

  asyncio.run(main(args=args))
