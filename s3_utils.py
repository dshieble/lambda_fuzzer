import logging
import os
from typing import Any, List, Optional
import uuid
import aioboto3 
from typing import List
from urllib.parse import urlparse
from tqdm import tqdm

from zerophishing.utilities.constants import AWS_REGION_NAME, AWS_PROFILE_NAME

class AsyncS3Client:

  def __init__(
    self,
    aws_region_name: str = AWS_REGION_NAME,
    aws_profile_name: str = AWS_PROFILE_NAME,
    buffer_length: int = 1000,
    aws_config_file_path: Optional[str] = None,
  ):
    if aws_config_file_path is not None:
      print(f"Setting AWS_CONFIG_FILE to {aws_config_file_path}")
      os.environ['AWS_CONFIG_FILE'] = aws_config_file_path

    self.session = aioboto3.Session(
      region_name=aws_region_name,
      profile_name=aws_profile_name
    )
    self.buffer_length = buffer_length
    self.s3_buffer = {}

  async def create_bucket_if_not_exists(self, s3_bucket_name: str):
    async with self.session.client('s3') as s3_client:
      # Check whether the bucket exists and create it if it does not
      try:
        await s3_client.head_bucket(Bucket=s3_bucket_name)
      except Exception as e:
        logging.error(f"S3 Bucket {s3_bucket_name} not found! Creating the bucket.")
        await s3_client.create_bucket(Bucket=s3_bucket_name)

  async def load_string_from_s3(self, s3_path: str) -> str:
    """
    Read the contents of s3 at s3_path into a string
    """
    raw_string = await self.load_object_from_s3(s3_path=s3_path)
    return raw_string.decode('utf-8')
  
  async def load_object_from_s3(self, s3_path: str) -> str:
    """
    Read the raw contents of s3 at s3_path
    """
    url_parts = urlparse(s3_path)
    s3_bucket_name = url_parts.netloc
    key = url_parts.path.lstrip('/')

    async with self.session.client('s3') as s3:
      response = await s3.get_object(Bucket=s3_bucket_name, Key=key)
      raw = await response['Body'].read()

    return raw

  async def write_object_to_s3(self, obj: Any, s3_path: str, run_create_bucket_if_not_exists: bool = False):
    """
    Write a single string to s3 at s3_path
    """
    url_parts = urlparse(s3_path)
    s3_bucket_name = url_parts.netloc
    key = url_parts.path.lstrip('/')

    if run_create_bucket_if_not_exists:
      await self.create_bucket_if_not_exists(s3_bucket_name=s3_bucket_name)

    async with self.session.client('s3') as s3_client:
      await s3_client.put_object(Body=obj, Bucket=s3_bucket_name, Key=key)


  async def load_string_list_from_s3(self, s3_path: str) -> List[str]:
    url_parts = urlparse(s3_path)
    bucket_name = url_parts.netloc
    prefix = url_parts.path.lstrip('/')

    async with self.session.resource('s3') as s3:
      bucket = await s3.Bucket(bucket_name)
      data = []

      async for obj in bucket.objects.filter(Prefix=prefix):
        lines = await (await obj.get())['Body'].readlines()
        data += [l.decode('utf-8') for l in lines]

    return data

  async def write_string_list_to_s3(self, string_list: List[str], s3_directory_path: str, run_create_bucket_if_not_exists: bool = False):
    """
    Given a list of strings, write them to a new file in the s3_directory_path
    """
    print(f"Writing string list of length {len(string_list)} to s3 directory path: {s3_directory_path}")
    url_parts = urlparse(s3_directory_path)
    s3_bucket_name = url_parts.netloc
    key = os.path.join(url_parts.path.lstrip('/'), str(uuid.uuid4()))

    if run_create_bucket_if_not_exists:
      await self.create_bucket_if_not_exists(s3_bucket_name=s3_bucket_name)

    body = "\n".join(string_list) + "\n"
    async with self.session.client('s3') as s3_client:
      await s3_client.put_object(Body=body, Bucket=s3_bucket_name, Key=key)

  async def write_buffer_to_s3(self, s3_directory_path: str):
    """
    Write the contents of the buffer to s3 for the given s3_directory_path
    """
    string_list = self.s3_buffer.get(s3_directory_path, [])
    await self.write_string_list_to_s3(string_list=string_list, s3_directory_path=s3_directory_path)
    self.s3_buffer[s3_directory_path] = []

  async def write_string_list_to_s3_buffer(self, string_list: List[str], s3_directory_path: str):
    """
    Given a list of strings, write them to the s3 buffer. Trigger a flush to s3 if the length is greater than the buffer length. We use this to avoid writing to s3 too often.
    """
    if len(string_list) > 0:
      if s3_directory_path not in self.s3_buffer:
        self.s3_buffer[s3_directory_path] = []
      self.s3_buffer[s3_directory_path] += string_list
      if len(self.s3_buffer[s3_directory_path]) >= self.buffer_length:
        await self.write_buffer_to_s3(s3_directory_path=s3_directory_path)
