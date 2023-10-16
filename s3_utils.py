import logging
import os
from typing import List
import uuid
import aioboto3 
from typing import List
from urllib.parse import urlparse
from tqdm import tqdm

class AsyncS3Client:

  def __init__(self, aws_region_name: str, aws_profile_name: str):
    os.environ['AWS_DEFAULT_REGION'] = aws_region_name
    self.session = aioboto3.Session(
      region_name=aws_region_name,
      profile_name=aws_profile_name
    )
    
  async def load_files_from_s3(self, s3_path: str) -> List[str]:
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

  async def create_bucket_if_not_exists(self, s3_bucket_name: str):
    async with self.session.client('s3') as s3_client:
      # Check whether the bucket exists and create it if it does not
      try:
        await s3_client.head_bucket(Bucket=s3_bucket_name)
      except Exception as e:
        logging.error(f"S3 Bucket {s3_bucket_name} not found! Creating the bucket.")
        await s3_client.create_bucket(Bucket=s3_bucket_name)


  async def write_string_list_to_s3(self, string_list: List[str], s3_directory_path: str):
    """
    Given a list of strings, write them to a new file in the s3_directory_path
    """
    if len(string_list) > 0:
      url_parts = urlparse(s3_directory_path)
      s3_bucket_name = url_parts.netloc
      key = os.path.join(url_parts.path.lstrip('/'), str(uuid.uuid4()))

      self.create_bucket_if_not_exists(s3_bucket_name=s3_bucket_name)
      url_list_string = "\n".join(string_list) + "\n"
      async with self.session.client('s3') as s3_client:
        await s3_client.put_object(Body=url_list_string, Bucket=s3_bucket_name, Key=key)
