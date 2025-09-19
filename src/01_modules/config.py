# import os
# import sys
# sys.path.append(os.path.abspath(os.getcwd()))
# from utils import version_test

_VERSION = '0.1'

AWS_REGION = 'eu-west-2' # London
ATHENA_WORKGROUP = 'primary'

DEFAULT_S3_BUCKET_NAME = 'sagemaker-research-methodology-extraction'
DEFAULT_S3_BUCKET_ROOT = f's3://{DEFAULT_S3_BUCKET_NAME}'

S3_DATA_PREFIX = '01_data'
S3_RAW_DATA_PREFIX = f'{S3_DATA_PREFIX}/01_raw'
S3_RAW_DATA_PATH = f'{DEFAULT_S3_BUCKET_ROOT}/{S3_RAW_DATA_PREFIX}'
S3_STG_DATA_PREFIX = f'{S3_DATA_PREFIX}/02_stg'
S3_STG_DATA_PATH = f'{DEFAULT_S3_BUCKET_ROOT}/{S3_STG_DATA_PREFIX}'
S3_CORE_DATA_PREFIX = f'{S3_DATA_PREFIX}/03_core'
S3_CORE_DATA_PATH = f'{DEFAULT_S3_BUCKET_ROOT}/{S3_CORE_DATA_PREFIX}'

DEFAULT_PROCESSING_FILEPATH_PREFIX = '/opt/ml/processing'

SEMANTICSCHOLAR_API_BASE_URL = 'https://api.semanticscholar.org/datasets/v1/release'
SEMANTICSCHOLAR_SECRET_NAME = 'semanticscholar_api_key'
SEMANTICSCHOLAR_SECRET_KEY = 'x-api-key'

OPENALEX_SOURCE_S3_BUCKET_NAME = 'openalex'
OPENALEX_S3_RAW_DATA_PREFIX = f'{S3_RAW_DATA_PREFIX}/openalex'

WANDB_ENTITY = 'steve-attila-kopias'
WANDB_PROJECT = 'sagemaker_research_classification'

print(f'config.py loaded: v{_VERSION}')