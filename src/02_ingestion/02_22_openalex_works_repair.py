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

parser = argparse.ArgumentParser()
parser.add_argument('--runtype', type=str, default='dev')
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print (args)

time_logger.log('Processed arguments')

s3_client = boto3.client('s3')
    
openalex_works_source_prefix = f'{config.OPENALEX_S3_RAW_DATA_PREFIX}/data/works'
openalex_works_target_prefix = f'{config.OPENALEX_S3_RAW_DATA_PREFIX}/data/works_unpartitioned'
openalex_works_source_path = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{openalex_works_source_prefix}'
openalex_works_target_path = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{openalex_works_target_prefix}'

# The source directory is partitioned, so it contains subdirectories like "updated_date=2025-08-18", and these subdirectories contain files like "part_000.gz", "part_001.gz", etc. Since the glue crawler is unable to parse this folder structure for some reason, we need to move all these files into the target directory without subdirectories. But to do that, we also need to rename all the files so they don't collide with each other.

"""
subdirectories = s3_client.list_objects(Bucket=config.DEFAULT_S3_BUCKET_NAME, Prefix=f'{openalex_works_source_prefix}/', Delimiter='/')
file_index = 0
for o in subdirectories.get('CommonPrefixes'):
    print('subdirectory: ', o.get('Prefix'))
    files = s3_client.list_objects(Bucket=config.DEFAULT_S3_BUCKET_NAME, Prefix=o.get('Prefix'), Delimiter='/')
    for file_content in files['Contents']:
        source_filepath = file_content['Key']
        new_key = f'{openalex_works_target_prefix}/{file_index:06}.gz'
        s3_client.copy_object(
            # Source bucket+filepath
            CopySource=f'{config.DEFAULT_S3_BUCKET_NAME}/{source_filepath}', 
            # Target bucket
            Bucket=config.DEFAULT_S3_BUCKET_NAME, 
            # Target filepath
            Key=new_key
        )
        print(f' - {source_filepath} -> {new_key}')
        file_index += 1
""" 
    
# for updated_date in utils.list_s3_prefixes(s3_client, openalex_works_source_prefix):
    # print(updated_date)
    # for part in utils.list_s3_objects(s3_client, f'{openalex_works_source_prefix}/{updated_date}'):
    #     new_key = f'{openalex_works_target_prefix}/{part.split("/")[-1]}'
    #     s3_client.copy_object(Bucket=config.DEFAULT_S3_BUCKET_NAME, CopySource=part, Key=new_key)
    #     s3_client.delete_object(Bucket=config.DEFAULT_S3_BUCKET_NAME, Key=part)

for entity in ['works_unpartitioned']:
    utils.glue_crawl(
        s3_targets=[f'{openalex_works_target_path}/{entity}'],
        database_name='01_raw',
        table_prefix='openalex_',
        aws_region=config.AWS_REGION,
        crawler_name=f'crawler_01_raw_openalex_{entity}'
    )
    time_logger.log(f'OpenAlex {entity} glue crawl done')


time_logger.log('DONE')