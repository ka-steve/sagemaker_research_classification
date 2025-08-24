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

parser = argparse.ArgumentParser()
parser.add_argument('--runtype', type=str, default='dev')
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print (args)

if RUNTYPE == 'dev':
    # PROCESSING_FILEPATH_PREFIX = '_dev_processing'
    # PROCESSING_FILEPATH_INPUT = os.path.join(script_dir, f'{PROCESSING_FILEPATH_PREFIX}/input/data/')
    # PROCESSING_FILEPATH_OUTPUT = os.path.join(script_dir, f'{PROCESSING_FILEPATH_PREFIX}/output/results/')
    USE_TQDM = True
elif RUNTYPE == 'prod':
    # PROCESSING_FILEPATH_PREFIX = config.DEFAULT_PROCESSING_FILEPATH_PREFIX
    # PROCESSING_FILEPATH_INPUT = f'{PROCESSING_FILEPATH_PREFIX}/input/data/'
    # PROCESSING_FILEPATH_OUTPUT = f'{PROCESSING_FILEPATH_PREFIX}/output/results/'
    USE_TQDM = False
else:
    raise ValueError('Argument --runtype should be either "dev" or "prod" (without quotes).')

time_logger.log('Processed arguments')


utils.glue_crawl(
    s3_targets=[f's3://{config.DEFAULT_S3_BUCKET_NAME}/01_data/01_raw/semanticscholar/'],
    database_name='01_raw',
    table_prefix='semanticscholar_',
    aws_region=config.AWS_REGION
)
time_logger.log('Semanticscholar glue crawl done')


time_logger.log('DONE')