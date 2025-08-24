import requests
import os
import sys
import argparse
import pathlib
import boto3
import time
import subprocess
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

def sync_aws_buckets(
    source_bucket:str,
    target_bucket:str,
    source_prefix:str='',
    target_prefix:str='',
)->None:
    """ Syncs the contents of one S3 bucket to another S3 bucket. Since boto3 does not have a sync function, we use the AWS CLI via subprocess. """
    command_down = f'aws s3 sync s3://{source_bucket}/{source_prefix} /tmp/{source_bucket}/ --source-region us-east-1'
    print(f'Running command_down: {command_down}')
    process_down = subprocess.Popen(command_down, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_down, stderr_down = process_down.communicate()
    command_up = f'aws s3 sync /tmp/{source_bucket}/ s3://{target_bucket}/{target_prefix} --region {config.AWS_REGION}'
    print(f'Running command_up: {command_up}')
    process_up = subprocess.Popen(command_up, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_up, stderr_up = process_up.communicate()
    if process_down.returncode != 0:
        print(f'Error syncing buckets: {stderr_down.decode()}')
    if process_up.returncode != 0:
        print(f'Error syncing buckets: {stderr_up.decode()}')
    else:
        print(f'Successfully synced buckets. Output: {stdout_up.decode()}')


parser = argparse.ArgumentParser()
parser.add_argument('--runtype', type=str, default='dev')
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print (args)

time_logger.log('Processed arguments')

# sync_aws_buckets(
#     source_bucket=config.OPENALEX_SOURCE_S3_BUCKET_NAME,
#     # source_prefix='data/publishers', # testing with a smaller subset first
#     target_bucket=config.DEFAULT_S3_BUCKET_NAME,
#     target_prefix=config.OPENALEX_S3_RAW_DATA_PREFIX,
# )

time_logger.log('OpenAlex files synced to target bucket')

s3_client = boto3.client('s3')
    
openalex_inner_data_prefix = f'{config.OPENALEX_S3_RAW_DATA_PREFIX}/data'
openalex_inner_data_path = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{openalex_inner_data_prefix}'
for entity in ['authors', 'concepts', 'domains', 'fields', 'funders', 'institutions', 'merged_ids', 'publishers', 'sources', 'subfields', 'topics', 'works']:
# for entity in ['publishers']:
    menifest_file_path = f'{openalex_inner_data_prefix}/{entity}/manifest'
    try:
        response = s3_client.delete_object(Bucket=config.DEFAULT_S3_BUCKET_NAME, Key=menifest_file_path)
        print(f"Object '{menifest_file_path}' deleted successfully from bucket '{config.DEFAULT_S3_BUCKET_NAME}'.")
    except Exception as e:
        print(f"Error deleting object '{menifest_file_path}': {e}")
    
    utils.glue_crawl(
        s3_targets=[f'{openalex_inner_data_path}/{entity}'],
        database_name='01_raw',
        table_prefix='openalex_',
        aws_region=config.AWS_REGION,
        crawler_name=f'crawler_01_raw_openalex_{entity}'
    )
    time_logger.log(f'OpenAlex {entity} glue crawl done')

# utils.glue_crawl(
#     s3_targets=[f'{openalex_inner_data_prefix}/authors'],
#     database_name='01_raw',
#     table_prefix='openalex_',
#     aws_region=config.AWS_REGION
# )
time_logger.log('OpenAlex glue crawl done')

time_logger.log('DONE')