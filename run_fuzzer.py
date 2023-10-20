import argparse
import asyncio
import numpy as np
from typing import Dict, List, Optional
import os
import uuid
import pybloomfilter
import aioboto3
from dataclasses import dataclass 

from lambda_fuzzer.proxy_manager import ProxyManager
from s3_utils import AsyncS3Client

# We add urls that have already been looked at to this bloom filter to avoid reprocessing them
BLOOM_FILTER_SIZE = 10000000000
BLOOM_FILTER_ERROR = 0.001


def load_bloom_filter(bloom_filter_path: Optional[str]) -> pybloomfilter.BloomFilter:
  """
  Load bloom filter from the bloom_filter_path if it exists, otherwise create a new one
  """
  bloom_filter_path = bloom_filter_path if bloom_filter_path is not None else f"/tmp/{uuid.uuid4()}.bloom"
  print(f"Using bloom_filter_path: {bloom_filter_path}")
  if os.path.exists(bloom_filter_path):
    bloom_filter = pybloomfilter.BloomFilter.open(bloom_filter_path)
  else:
    bloom_filter = pybloomfilter.BloomFilter(BLOOM_FILTER_SIZE, BLOOM_FILTER_ERROR, bloom_filter_path)
  return bloom_filter

@dataclass
class UrlWriter:
  bloom_filter: pybloomfilter.BloomFilter
  async_s3_client: AsyncS3Client

  @classmethod
  async def create_url_writer(
    cls,
    aws_profile_name: str,
    bloom_filter_path: str,
    aws_region_name: str
  ):
    async_s3_client = AsyncS3Client(aws_profile_name=aws_profile_name, aws_region_name=aws_region_name)
    
    return cls(
      bloom_filter=load_bloom_filter(bloom_filter_path=bloom_filter_path),
      async_s3_client=async_s3_client,
    )

  def write_url_list_to_bloom(self, url_list: List[str]):
    for url in url_list:
      self.bloom_filter.update((url,))

  async def get_filtered_urls(self, url_list: List[str], s3_path: Optional[str] = None) -> List[str]:
    """
    Get all urls that weren't already written to s3 or the bloom filter
    """
    processed_url_set = set(
      await self.async_s3_client.load_string_list_from_s3(s3_path=s3_path))  if s3_path is not None else set()
    print(f"Loaded {len(processed_url_set)} already processed urls from s3_path: {s3_path}")
    return [url for url in url_list if url not in self.bloom_filter and url not in processed_url_set]

  async def write_url_list_to_s3(self, url_list: List[str], s3_path: str):
    await self.async_s3_client.write_string_list_to_s3_buffer(string_list=url_list, s3_directory_path=s3_path)

  async def get_urls_to_scan_from_fuzz_terms_file(
    self,
    url_template: str,
    path_to_fuzz_terms_file: str,
    s3_path: Optional[str] = None,
  ) -> List[str]:
    """
    Args:
      url_template: The url template to inject fuzz terms into
      path_to_fuzz_terms_file: The path to the file containing the fuzz terms to use
      s3_path: The path to the urls that have already been written
    Returns:
      url_list: The list of urls to scan
    """
    with open(path_to_fuzz_terms_file, 'r') as f:
      fuzz_term_list = f.readlines()
    full_url_list = [url_template % fuzz_term.strip() for fuzz_term in fuzz_term_list]
    
    url_list = await self.get_filtered_urls(url_list=full_url_list, s3_path=s3_path)
    print(f"Fuzzing {len(url_list)} urls out of {len(full_url_list)} total urls.")
    
    example_urls = '\n'.join(url_list[:10])
    print(f"Example Urls: {example_urls}")
    return url_list



async def execute_proxy_discovery_on_url_list(
  url_list: List[str],
  proxy_manager: ProxyManager,
  proxy_number: int,
  url_writer: UrlWriter,
  s3_path: str
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
      print(
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

      print(f"Writing {len(success_code_urls)} success code urls to s3 and {len(url_list)} total urls to bloom filter")
      await url_writer.write_url_list_to_s3(url_list=success_code_urls, s3_path=s3_path)
      url_writer.write_url_list_to_bloom(url_list=url_list)



async def main(args):

  url_writer = await UrlWriter.create_url_writer(
    aws_profile_name=args.aws_profile_name,
    aws_region_name=args.aws_region_name,
    bloom_filter_path=args.bloom_filter_path)

  url_template_list = args.url_template_list.split(',')
  s3_path_list = args.s3_path_list.split(',')
  if len(url_template_list) != len(s3_path_list):
    raise ValueError(f"Length of url_template_list: {len(url_template_list)} must equal length of s3_directory_key_list: {len(s3_path_list)}")

  for url_template, s3_path in zip(url_template_list, s3_path_list):
    print(f"Processing url template: {url_template} and writing to s3_path: {s3_path}")
    full_url_list = await url_writer.get_urls_to_scan_from_fuzz_terms_file(
      url_template=url_template, path_to_fuzz_terms_file=args.path_to_fuzz_terms_file, s3_path=s3_path)
    print(f"Loaded {len(full_url_list)} urls to process for url_template: {url_template}")

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
          url_writer=url_writer,
          s3_path=s3_path
        )
        for proxy_number, url_list in zip(range(int(args.min_proxy), int(args.max_proxy)), chunked_url_list[i:i+num_proxies])
      ])

      # Flush the buffer to s3
      await url_writer.async_s3_client.write_buffer_to_s3(s3_directory_path=s3_path)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--path_to_fuzz_terms_file", type=str, required=True, help="the path to the file containing the fuzz terms to use")
  parser.add_argument("--s3_path_list", type=str, required=True, help="the list of paths to the directories on s3 where the files containing discovered urls will be written")
  parser.add_argument("--url_template_list", type=str, required=True, help="the comma-separated list of url templates to use, with `%s` in place of the term to fuzz. Must be the same length as s3_directory_key_list")


  parser.add_argument("--bloom_filter_path", type=str, default=None, help="the path to the bloom filter to use. If not provided, a new one will be created")
  parser.add_argument("--number_urls_to_process_per_proxy_call", type=int, default=100, help="the number of urls to process within each call to a lambda proxy")

  parser.add_argument("--aws_profile_name", type=str, required=True, help="the name of the aws profile to use")
  parser.add_argument("--aws_region_name", type=str, default="us-east-1", help="the name of the aws region to use")
  parser.add_argument("--min_proxy", type=int, default=0, help="the minimum proxy number to use")
  parser.add_argument("--max_proxy", type=int, default=10, help="the maximum proxy number to use")

  args = parser.parse_args()

  asyncio.run(main(args=args))
