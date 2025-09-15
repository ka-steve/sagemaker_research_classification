import requests
import os
import sys
import argparse
import pathlib
import boto3
import time
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from tqdm import tqdm
from typing import List, Union, Optional


# Adding ../01_modules or ./01_modules to the system path so that we can load modules from 
# there as well
if '__file__' in globals():
    script_dir = pathlib.Path(__file__).parent.resolve()
else:
    script_dir = pathlib.Path().absolute()
modules_path_in_dev = os.path.abspath(os.path.join(script_dir, '..', '01_modules'))
modules_path_in_prod = os.path.abspath(os.path.join(script_dir, '01_modules'))
if os.path.exists(modules_path_in_dev):
    sys.path.append(modules_path_in_dev)
if os.path.exists(modules_path_in_prod):
    sys.path.append(modules_path_in_prod)

import utils
import config

time_logger = utils.TimeLogger()

semanticscholar_secret = utils.get_secret(config.AWS_REGION, config.SEMANTICSCHOLAR_SECRET_NAME)
SEMANTICSCHOLAR_API_KEY = semanticscholar_secret[config.SEMANTICSCHOLAR_SECRET_KEY]
print(f'Keys stored in Secrets Managers for the secret "{config.SEMANTICSCHOLAR_SECRET_NAME}":', list(semanticscholar_secret.keys()))
time_logger.log('Semanticscholar secret keys fetched')

class SemanticScholarIngestion():
    """Class to handle the ingestion of datasets from Semantic Scholar.

        Args:
            release_id (str): The release ID (like '2025-08-12') of the dataset to fetch the file URLs.
            dataset_id (str): The dataset ID (like 's2orc') to fetch the file URLs and to use as the prefix for the target key.
            target_bucket (str): The name of the target S3 bucket.
            target_s3_prefix (str): The prefix in the target S3 bucket where the file will be uploaded.
            aws_region (str): The AWS region of the target S3 bucket.
            force_overwrite (bool): If False, skip downloading and uploading if the file already exists in S3.
            use_tqdm (bool): If True, show a progress bar during download. This is only practical during development when executing this python script directly. In production runs (e.g. via SageMaker Processing Jobs), this should be False, otherwise every update of the progress bar will be logged as a separate line in the CloudWatch logs.
            min_index (int): The starting index boundary (inclusive) of the file to process.
            max_index (int): The ending index boundary (exclusive) of the file to process.

        Returns:
            None
        """
    def __init__(
        self, 
        release_id:str, 
        dataset_id:str,
        target_bucket:str,
        target_s3_prefix:str='01_raw',
        aws_region:str='eu-west-2',
        force_overwrite:bool=False,
        use_tqdm:bool=False,
        min_index:int=0,
        max_index:int=10000000,
    ) -> None:
        self.release_id:str = release_id
        self.dataset_id:str = dataset_id
        self.target_bucket:str = target_bucket
        self.target_s3_prefix:str = target_s3_prefix
        self.aws_region:str = aws_region
        self.force_overwrite:bool = force_overwrite
        self.use_tqdm:bool = use_tqdm
        self.file_urls:list = []
        self.next_file_counter:Optional[int] = None
        self.min_index:int = min_index
        self.max_index:int = max_index
        
        self.download_and_upload_next_file_to_s3()
        
    def retrieve_dataset_download_urls(self):
        """Fetch the download URL for a specific dataset.
        
        Args:
            release_id (str): The release ID (like '2025-08-12') of the dataset to fetch the file URLs.
            dataset_id (str): The dataset ID (like 's2orc') to fetch the file URLs.
        """
        url = f'{config.SEMANTICSCHOLAR_API_BASE_URL}/{self.release_id}/dataset/{self.dataset_id}'
        headers={'x-api-key': SEMANTICSCHOLAR_API_KEY}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        self.file_urls = response.json()['files']

    def download_and_upload_next_file_to_s3(self, is_retry:bool=False)-> None:
        """Downloads the next file from the dataset and uploads it to the target S3 bucket.
        """
        if self.next_file_counter is None:
            self.retrieve_dataset_download_urls()
            self.next_file_counter = max(0, self.min_index)
            
        if self.next_file_counter >= len(self.file_urls):
            print('All files have been processed.')
            return
        
        if self.next_file_counter >= self.max_index:
            print(f'Reached the maximum index limit of {self.max_index}. Stopping further processing.')
            return

        url = self.file_urls[self.next_file_counter]
        # Extract the filename from the URL
        target_filename = f'{self.dataset_id}-part{self.next_file_counter}.jsonl.gz'
        target_key = f'{self.target_s3_prefix}/{self.dataset_id}/{target_filename}'

        # Step 1: Check if the file already exists in S3
        s3_client = boto3.client('s3', region_name=self.aws_region)
        if not self.force_overwrite:
            try:
                s3_client.head_object(Bucket=self.target_bucket, Key=target_key)
                print(f'File already exists in S3: s3://{self.target_bucket}/{target_key}. Skipping download and upload.')
                self.next_file_counter += 1
                self.download_and_upload_next_file_to_s3()
                return
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    print(f'Error checking file in S3: {e}')
                    return

        # Step 2: Download the file locally (with a progress bar if needed)
        local_file = target_filename  # Use the filename as the local file name
        try:
            print(f'Downloading file from {url}...')
            response = requests.get(url, stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))  # Get the total file size

            if self.use_tqdm: 
                with open(local_file, 'wb') as file, tqdm(
                    total=total_size, unit='B', unit_scale=True, desc='Downloading'
                ) as progress_bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                        progress_bar.update(len(chunk))  # Update the progress bar
            else:
                with open(local_file, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
            print(f'File downloaded: {local_file}')
        except Exception as e:
            print(f'Error downloading file: {e}')
            if is_retry:
                # If this was already a retry, do not attempt again to avoid infinite loops
                raise e
            else:
                print('Refreshing signed URL list and retrying download...')
                self.retrieve_dataset_download_urls()
                self.download_and_upload_next_file_to_s3(is_retry=True)
        else:
            # Step 3: Upload the file to the target S3 bucket
            try:
                print(f'Uploading {local_file} to S3 bucket {self.target_bucket}...')
                s3_client.upload_file(local_file, self.target_bucket, target_key)
                print(f'File uploaded to S3: s3://{self.target_bucket}/{target_key}')
            except Exception as e:
                print(f'Error uploading file to S3: {e}')
            finally:
                # Clean up local file
                if os.path.exists(local_file):
                    os.remove(local_file)
            # Increment the counter for the next file
            self.next_file_counter += 1
            self.download_and_upload_next_file_to_s3()


parser = argparse.ArgumentParser()
parser.add_argument('--runtype', type=str, default='dev')
parser.add_argument('--dataset-id', type=str, default='s2orc')
parser.add_argument('--release-id', type=str, default='2025-08-12')
parser.add_argument('--force-overwrite', type=bool, default=False)
parser.add_argument('--min-index', type=int, default=0)
parser.add_argument('--max-index', type=int, default=2)
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print (args)

if RUNTYPE == 'dev':
    USE_TQDM = True
elif RUNTYPE == 'prod':
    USE_TQDM = False
else:
    raise ValueError('Argument --runtype should be either "dev" or "prod" (without quotes).')

time_logger.log('Processed arguments')


ss_ingestion = SemanticScholarIngestion(
    release_id=args.release_id,
    dataset_id=args.dataset_id,
    target_bucket=config.DEFAULT_S3_BUCKET_NAME,
    target_s3_prefix='01_data/01_raw/semanticscholar',
    force_overwrite=args.force_overwrite,
    use_tqdm=USE_TQDM,
    aws_region=config.AWS_REGION,
    min_index=args.min_index,
    max_index=args.max_index
)

time_logger.log('Semanticscholar files downloaded')

time_logger.log('DONE')