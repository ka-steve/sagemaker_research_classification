import requests
import os
import sys
import json
import argparse
import importlib
import subprocess
import pathlib
import boto3
import time
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from tqdm import tqdm
from typing import List, Union


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

def semanticscholar_get_release_ids():
    """Fetching the list of dataset release IDs."""
    response = requests.get(config.SEMANTICSCHOLAR_API_BASE_URL)
    response.raise_for_status()
    res = response.json()
    print(res)
    return res


def semanticscholar_get_latest_metadata(release_id):
    """Fetch the metadata for the latest dataset release."""
    url = f'{config.SEMANTICSCHOLAR_API_BASE_URL}/{release_id}'
    response = requests.get(url)
    response.raise_for_status()
    res_json = response.json()
    res = []
    for dataset in res_json['datasets']:
        if dataset['name'] in ['papers', 's2orc']:
            res.append(dataset)
            print(dataset)
    return res


def get_releases_and_metadata():
    release_ids = semanticscholar_get_release_ids()
    latest_release_id = release_ids[-1]

    semanticscholar_get_latest_metadata(latest_release_id)
    return latest_release_id

def get_dataset_download_urls(release_id, dataset_name):
    """Fetch the download URL for a specific dataset."""
    url = f'{config.SEMANTICSCHOLAR_API_BASE_URL}/{release_id}/dataset/{dataset_name}'
    response = requests.get(url, headers={'x-api-key': SEMANTICSCHOLAR_API_KEY}
)
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2, default=str))
    return response.json()['files']

def download_and_upload_to_s3(
    counter: int,
    url:str,
    dataset_id:str,
    target_bucket:str,
    target_s3_prefix:str='01_raw',
    aws_region:str="eu-west-2",
    force_overwrite:bool=False
)-> None:
    """
    Downloads a file from a signed S3 URL and uploads it to another S3 bucket.

    Args:
        counter (int): The index of the file in the list.
        url (str): The signed S3 URL of the file to download.
        dataset_id (str): The dataset ID to use as the prefix for the target key.
        target_bucket (str): The name of the target S3 bucket.
        target_s3_prefix (str): The prefix in the target S3 bucket where the file will be uploaded.
        aws_region (str): The AWS region of the target S3 bucket.
        force_overwrite (bool): If False, skip downloading and uploading if the file already exists in S3.

    Returns:
        None
    """
    # Extract the filename from the URL
    filename = f"{url.split('/')[-1].split('?')[0]}"
    target_filename = f'{dataset_id}-part{counter}.jsonl.gz'
    target_key = f"{target_s3_prefix}/{dataset_id}/{target_filename}"

    # Step 1: Check if the file already exists in S3
    s3_client = boto3.client("s3", region_name=aws_region)
    if not force_overwrite:
        try:
            s3_client.head_object(Bucket=target_bucket, Key=target_key)
            print(f"File already exists in S3: s3://{target_bucket}/{target_key}. Skipping download and upload.")
            return
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                print(f"Error checking file in S3: {e}")
                return

    # Step 2: Download the file locally with a progress bar
    local_file = target_filename  # Use the filename as the local file name
    try:
        print(f"Downloading file from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))  # Get the total file size
    
        with open(local_file, "wb") as file, tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Downloading"
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
                progress_bar.update(len(chunk))  # Update the progress bar
        print(f"File downloaded: {local_file}")
    except Exception as e:
        print(f"Error downloading file: {e}")
        return

    # Step 3: Upload the file to the target S3 bucket
    try:
        print(f"Uploading {local_file} to S3 bucket {target_bucket}...")
        s3_client.upload_file(local_file, target_bucket, target_key)
        print(f"File uploaded to S3: s3://{target_bucket}/{target_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
    finally:
        # Clean up local file
        if os.path.exists(local_file):
            os.remove(local_file)


def process_files(
    file_urls:list,
    dataset_id:str,
    target_bucket:str,
    target_s3_prefix:str='01_raw',
    force_overwrite:bool=False,
    min_index:int=0,
    max_index:int=100000,
    aws_region:str="eu-west-2"
)->None:
    """
    Processes a list of signed S3 URLs and uploads them to the target S3 bucket.

    Args:
        file_urls (list): List of signed S3 URLs.
        dataset_id (str): The dataset ID to use as the prefix for the target key.
        target_bucket (str): The name of the target S3 bucket.
        target_s3_prefix (str): The prefix in the target S3 bucket where the files will be uploaded.
        min_index (int): The starting index boundary (inclusive) of the file to process.
        max_index (int): The ending index boundary (exclusive) of the file to process.
        aws_region (str): The AWS region of the target S3 bucket.
        force_overwrite (bool): If False, skip downloading and uploading if the file already exists in S3.

    Returns:
        None
    """
    counter = 0
    for url in file_urls:
        if counter >= min_index and counter < max_index:
            download_and_upload_to_s3(
                counter=counter,
                url=url,
                dataset_id=dataset_id,
                target_bucket=target_bucket,
                target_s3_prefix=target_s3_prefix,
                aws_region=aws_region,
                force_overwrite=force_overwrite
            )
        counter += 1
    print('DONE')

def glue_crawl(s3_targets:Union[List[str], str], database_name:str, table_prefix:str, aws_region:str="eu-west-2")->None:
    """
    Runs a Glue Crawler to refresh the Glue Catalog.

    Args:
        s3_targets (Union[List[str], str]): S3 paths to crawl. Can be a single path or a list of paths.
        database_name (str): The name of the Glue database where the table should be created.
        table_prefix (str): The prefix for the table name in the Glue database.
        aws_region (str): The AWS region where the Glue Crawler is located.

    Returns:
        None
    """
    glue_client = boto3.client("glue", region_name=aws_region)

    if isinstance(s3_targets, str):
        crawler_targets = [{'Path': s3_targets}]
    elif isinstance(s3_targets, list):
        crawler_targets = [{'Path': target} for target in s3_targets]

    crawler_name = f'crawler_{database_name}_{table_prefix}'
    
    crawler_config = {
        'Name': crawler_name,
        'Role': 'AWSGlueServiceRoleDefault',
        'DatabaseName': database_name,
        'Targets': {
            'S3Targets': crawler_targets
        },
        'TablePrefix': table_prefix
    }

    #Attempt to create and start a glue crawler on PSV table or update and start it if it already exists.
    try:
        glue_client.create_crawler(**crawler_config)
    except:
        glue_client.update_crawler(**crawler_config)
        
    try:
        print(f"Starting Glue Crawler: {crawler_name}...")
        glue_client.start_crawler(Name=crawler_name)
        glue_is_running = True
        # Wait for the crawler to complete
        while glue_is_running:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response["Crawler"]["State"]
            if state == "READY":
                print(f"Glue Crawler {crawler_name} has completed.")
                glue_is_running = False
            elif state == "RUNNING":
                print(f"Glue Crawler {crawler_name} is still running...")
            else:
                print(f"Unexpected state for Glue Crawler {crawler_name}: {state}")
                break
            time.sleep(10)  # Wait for 10 seconds before checking again

    except ClientError as e:
        print(f"Error running Glue Crawler {crawler_name}: {e}")

SEMANTICSCHOLAR_LATEST_RELEASE_ID = get_releases_and_metadata()
print(f'SEMANTICSCHOLAR_LATEST_RELEASE_ID: {SEMANTICSCHOLAR_LATEST_RELEASE_ID}')
time_logger.log('Semanticscholar latest release ID fetched')

parser = argparse.ArgumentParser()
parser.add_argument("--runtype", type=str, default='dev')
parser.add_argument("--test-argument-key-01", type=str, default='test-argument-default-value-01')
parser.add_argument("--test-argument-key-02", type=str, default='test-argument-default-value-02')
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print (args)

if RUNTYPE == 'dev':
    PROCESSING_FILEPATH_PREFIX = '_dev_processing'
    PROCESSING_FILEPATH_INPUT = os.path.join(script_dir, f'{PROCESSING_FILEPATH_PREFIX}/input/data/')
    PROCESSING_FILEPATH_OUTPUT = os.path.join(script_dir, f'{PROCESSING_FILEPATH_PREFIX}/output/results/')
elif RUNTYPE == 'prod':
    PROCESSING_FILEPATH_PREFIX = config.DEFAULT_PROCESSING_FILEPATH_PREFIX
    PROCESSING_FILEPATH_INPUT = f'{PROCESSING_FILEPATH_PREFIX}/input/data/'
    PROCESSING_FILEPATH_OUTPUT = f'{PROCESSING_FILEPATH_PREFIX}/output/results/'
else:
    raise ValueError('Argument --runtype should be either "dev" or "prod" (without quotes).')


utils.ensure_path(PROCESSING_FILEPATH_INPUT)
utils.ensure_path(PROCESSING_FILEPATH_OUTPUT)

with open(os.path.join(PROCESSING_FILEPATH_INPUT, 'test.txt'), "r", encoding='utf-8') as test_file:
    test_content = test_file.read()

utcnow_string = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
target_filepath = os.path.join(PROCESSING_FILEPATH_OUTPUT, f'results__{utcnow_string}.txt')
print(f'Target filepath for results: {target_filepath}')

with open(target_filepath, "w", encoding='utf-8') as results_file:
    results_file.write('\n'.join([
        f'semanticscholar_secret_keys[0]: {list(semanticscholar_secret.keys())[0]}',
        f'SEMANTICSCHOLAR_LATEST_RELEASE_ID: {SEMANTICSCHOLAR_LATEST_RELEASE_ID}',
        f'test_content: {test_content}',
        f'args.test_argument_key_01: {args.test_argument_key_01}',
        f'args.test_argument_key_02: {args.test_argument_key_02}'
    ]))


dataset_id = 's2orc'
release_id = '2025-08-12'
s2orc_file_urls = get_dataset_download_urls(release_id, dataset_id)
process_files(
    file_urls=s2orc_file_urls,
    dataset_id=dataset_id,
    target_bucket=config.DEFAULT_S3_BUCKET_NAME,
    target_s3_prefix='01_raw/semanticscholar',
    force_overwrite=False,
    min_index=0,
    max_index=100
)
glue_crawl(
    s3_targets=[f's3://{config.DEFAULT_S3_BUCKET_NAME}/01_raw/semanticscholar/'],
    database_name='01_raw',
    table_prefix='semanticscholar_',
    aws_region=config.AWS_REGION
)


time_logger.log('DONE')