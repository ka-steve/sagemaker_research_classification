import boto3
import json
import os
import pathlib
import shutil
import tarfile
import time
import awswrangler as wr
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from sagemaker import image_uris
from sagemaker.session import get_execution_role
from typing import Union, List, Optional

import config


def version_test() -> str:
    # Helper util to confirm that the last version of the helper_utils.py is loaded
    version = '0.2.12'
    # print(version)
    return version


def pd_set_options(rows=500, cols=1000):
    # Don't hide any columns when printing
    pd.set_option("display.max_columns", None)
    # Show all rows if there are no more than:
    pd.set_option("display.max_rows", rows)
    # ...otherwise show this many rows:
    pd.set_option("display.min_rows", rows)
    # Show the full content of columns:
    pd.set_option("display.max_colwidth", cols)
    # Display the table assuming this screen width
    pd.set_option("display.width", 800)


class TimeLogger:
    def __init__(self):
        self.timelog = []
        self.log(':: TIMELOGGER STARTED ::')

    def format_seconds(self, total_seconds:float)->str:
        seconds_in_minute = 60
        seconds_in_hour = seconds_in_minute * 60
        seconds_in_day = seconds_in_hour * 24
        days = total_seconds//seconds_in_day
        hours = (total_seconds - days*seconds_in_day)//seconds_in_hour
        minutes = (total_seconds - days*seconds_in_day - hours*seconds_in_hour)//seconds_in_minute
        seconds = total_seconds - days*seconds_in_day - hours*seconds_in_hour - minutes*seconds_in_minute
        result = ""
        if days:
            result += f"{days} day{'s' if days!=1 else ''}, "
        if days or hours:
            result += f"{hours} hour{'s' if hours!=1 else ''}, "
        if days or hours or minutes:
            result += f"{minutes} minute{'s' if minutes!=1 else ''}, "
        result += f"{seconds:.2f} second{'s' if seconds!=1 else ''}"
        return result
    
    def format(self, log_dict:dict[str, Union[str, float]])->str:
        return f" :: {log_dict['message']} | since_start: {self.format_seconds(float(log_dict['since_start']))} | since_last: {self.format_seconds(float(log_dict['since_last']))} :: "

    def log(self, message:str, do_print:bool=True)->str:
        now = time.perf_counter()
        time_display = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if len(self.timelog) == 0:
            log_dict = {
                'message': message,
                'time_display': time_display,
                'time_perf': now,
                'since_start': 0.0,
                'since_last': 0.0
            }
            self.timelog.append(log_dict)
        else:
            last_time = self.timelog[-1]['time_perf']
            since_last = now - last_time
            since_start = now - self.timelog[0]['time_perf']
            log_dict = {
                'message': message,
                'time_display': time_display,
                'time_perf': now,
                'since_start': since_start,
                'since_last': since_last
            }
            self.timelog.append(log_dict)
            
        formatted_log = self.format(log_dict)
        if do_print:
            print(formatted_log)
            
        return formatted_log


def get_secret(region_name:str, secret_name:str)->dict[str, str]:
    # Source of the function:
    # https://eu-west-2.console.aws.amazon.com/secretsmanager/secret?name=semanticscholar_api_key#secret-details-sample-code-section
    # Create a Secrets Manager client
    session = boto3.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret


def ensure_path(path:str)->None:
    from pathlib import Path
    print(f'ensure_path({path})')
    Path(path).mkdir(parents=True, exist_ok=True)


def glue_crawl(s3_targets:Union[List[str], str], database_name:str, table_prefix:str, aws_region:str='eu-west-2', crawler_name:Optional[str]=None)->None:
    """
    Creates (or updates) and starts an AWS Glue Crawler to crawl the specified S3 paths.

    Args:
        s3_targets (Union[List[str], str]): S3 paths to crawl. Can be a single path or a list of paths.
        database_name (str): The name of the Glue database where the table should be created.
        table_prefix (str): The prefix for the table name in the Glue database.
        aws_region (str): The AWS region where the Glue Crawler is located.

    Returns:
        None
    """
    glue_client = boto3.client('glue', region_name=aws_region)

    if isinstance(s3_targets, str):
        crawler_targets = [{'Path': s3_targets}]
    elif isinstance(s3_targets, list):
        crawler_targets = [{'Path': target} for target in s3_targets]
    else:
        raise ValueError('s3_targets must be a string or a list of strings.')
    
    print(f'Starting to crawl S3 targets: {crawler_targets} into database: {database_name} with table prefix: {table_prefix}')

    if crawler_name is None:
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

    # Attempt to create a glue crawler or update it if it already exists.
    try:
        glue_client.create_crawler(**crawler_config)
    except:
        glue_client.update_crawler(**crawler_config)
        
    try:
        print(f'Starting Glue Crawler: {crawler_name}...')
        glue_client.start_crawler(Name=crawler_name)
        glue_is_running = True
        # Wait for the crawler to complete
        while glue_is_running:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            if state == 'READY':
                print(f'Glue Crawler {crawler_name} has completed.')
                glue_is_running = False
            elif state in ['RUNNING', 'STOPPING']:
                print(f'Glue Crawler {crawler_name} is still running...')
            else:
                print(f'Unexpected state for Glue Crawler {crawler_name}: {state}')
                break
            time.sleep(10)  # Wait for 10 seconds before checking again

    except ClientError as e:
        print(f'Error running Glue Crawler {crawler_name}: {e}')

def create_table_from_sql_file(
    database_name:str, 
    table_name:str, 
    overwrite_strategy:str='fail', # options: fail, overwrite, ignore
    wait:bool=True,
    s3_parent_target_path:Optional[str]=None
)->None:
    sql_file_path = os.path.join('sql', f'{database_name}__{table_name}.sql')
    if not os.path.exists(sql_file_path):
        raise ValueError(f'ERROR: SQL file {sql_file_path} does not exist.')
    
    if s3_parent_target_path is None:
        s3_parent_target_path = config.S3_STG_DATA_PATH
        
    wr.catalog.create_database(name=database_name, exist_ok=True)
    
    table_exists = wr.catalog.does_table_exist(database=database_name, table=table_name)
    if table_exists:
        if overwrite_strategy == 'fail':
            raise ValueError(f'ERROR: Table {database_name}.{table_name} already exists.')
        elif overwrite_strategy == 'ignore':
            print(f'Table {database_name}.{table_name} already exists. Ignoring since overwrite_strategy=="ignore".')
            return
        elif overwrite_strategy == 'overwrite':
            print(f'Table {database_name}.{table_name} already exists. Overwriting since overwrite_strategy=="overwrite".')
            print('Deleting table from Glue Catalog', database_name, table_name)
            wr.catalog.delete_table_if_exists(database=database_name, table=table_name)
        else:
            raise ValueError(f'Invalid overwrite_strategy: {overwrite_strategy}. Choose from "fail", "overwrite", "ignore".')
    if overwrite_strategy == 'overwrite':
        print('Deleting S3 objects from', f'{s3_parent_target_path}/{table_name}/')
        wr.s3.delete_objects(path=f'{s3_parent_target_path}/{table_name}/')
        
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()
        
    print('s3_parent_target_path: ', s3_parent_target_path)
    wr.athena.create_ctas_table(
        sql=sql_query,
        database=database_name,
        wait=wait,
        ctas_table=table_name,
        s3_output=s3_parent_target_path,
    )

def create_supervised_multiclass_classification_training_job(
    SCRIPT_FILEPATH,
    MODEL_NAME,
    INSTANCE_TYPE,
    LABEL_TYPE,
    TEXT_KEY,
    SAMPLE,          # must be string
    MAX_RUNTIME_S,
    TRAIN_BATCH_SIZE=16,
    EVAL_BATCH_SIZE=32,
    WARMUP_STEPS=100,
    LEARNING_RATE=5e-5,
    ENTRY_POINT='05_tuning_basic/05_12_tuning_basic_simple.py',
    TEXT_KEY_RENAME_TO='text',
    LABEL_KEY_RENAME_TO='label',
    VOLUME_SIZE_GB=None
):

    SAMPLE = str(SAMPLE)
    TRAIN_BATCH_SIZE=str(TRAIN_BATCH_SIZE)
    EVAL_BATCH_SIZE=str(EVAL_BATCH_SIZE)
    WARMUP_STEPS=str(WARMUP_STEPS)
    LEARNING_RATE=str(LEARNING_RATE)
    if not VOLUME_SIZE_GB:
        if INSTANCE_TYPE == 'ml.g6.xlarge':
            VOLUME_SIZE_GB=250
        else:
            VOLUME_SIZE_GB=450
            
    model_short_name = MODEL_NAME.split("/")[-1].split("-")[0]
    NOW = datetime.now().strftime('%m%d%H%M%S')
    JOB_NAME = f'{model_short_name}-{LABEL_TYPE}-{TEXT_KEY}-s{SAMPLE}-{NOW}'
    MODEL_SHORT_NAME = model_short_name
    SAGEMAKER_CLIENT = boto3.client('sagemaker', region_name=config.AWS_REGION)
    S3_CLIENT = boto3.client('s3')
    EXECUTION_ROLE = get_execution_role()
    SOURCE_DIRPATH = SCRIPT_FILEPATH.parents[0]
    ROOT_DIRPATH = SCRIPT_FILEPATH.parents[1]
    TEMP_DIRPATH = pathlib.Path(f'./_code/{JOB_NAME}')
    TAR_FILEPATH = pathlib.Path(f'./_tar/source-{JOB_NAME}.tar.gz')
    ENV_VARS = {
        'HUGGINGFACE_HUB_CACHE': '/tmp/.cache'
    }

    if TEMP_DIRPATH.parents[0].exists():
        shutil.rmtree(TEMP_DIRPATH.parents[0])
    TEMP_DIRPATH.mkdir(parents=True, exist_ok=True)

    if TAR_FILEPATH.parents[0].exists():
        shutil.rmtree(TAR_FILEPATH.parents[0])
    TAR_FILEPATH.parents[0].mkdir(parents=True, exist_ok=True)

    ignore_names = {'__pycache__', '.ipynb_checkpoints'}
    for item in SOURCE_DIRPATH.iterdir():
        name = item.name
        if name in ignore_names:
            continue
        dest = TEMP_DIRPATH / name
        if item.is_dir():
            # print('item.is_dir()', item, dest)
            for item2 in item.iterdir():
                name2 = item2.name
                if name2 in ignore_names:
                    continue
                dest2 = TEMP_DIRPATH / name / name2
                if item2.is_dir():
                    pass
                else:
                    dest.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(item2, dest2)
        else:
            shutil.copy2(item, dest)

    shutil.copy2(ROOT_DIRPATH / 'requirements_train.txt', TEMP_DIRPATH / 'requirements.txt')

    # Tar the temp_dir (its contents become root of /opt/ml/code)
    with tarfile.open(TAR_FILEPATH, 'w:gz') as tar:
        tar.add(str(TEMP_DIRPATH), arcname='.')

    code_s3_key = f'02_code/train/{JOB_NAME}/source.tar.gz'
    S3_CLIENT.upload_file(str(TAR_FILEPATH), config.DEFAULT_S3_BUCKET_NAME, code_s3_key)
    code_s3_uri = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{code_s3_key}'

    image_uri = image_uris.retrieve(
        framework='huggingface',
        region=config.AWS_REGION,
        version='4.49.0',                 # transformers version
        py_version='py311',
        instance_type=INSTANCE_TYPE,
        image_scope='training',
        base_framework_version='pytorch2.5.1'
    )
    # p_rint('Using training image:', image_uri)
    
    hyperparameters = {
        # SageMaker training toolkit special keys:
        'sagemaker_program': ENTRY_POINT,
        'sagemaker_submit_directory': code_s3_uri,
        'sagemaker_container_log_level': '20',
        'sagemaker_region': config.AWS_REGION,
    
        # Your script args:
        'runtype': 'prod',
        'now': NOW,
        'instance_type': INSTANCE_TYPE,
        'model_name': MODEL_NAME,
        'model_short_name': MODEL_SHORT_NAME,
        
        'label_type': LABEL_TYPE,
        'text_key': TEXT_KEY,
        'text_key_rename_to': TEXT_KEY_RENAME_TO,
        'label_key_rename_to': LABEL_KEY_RENAME_TO,
        'sample': SAMPLE,          # must be string
        'epochs': '5',
        'train_batch_size': TRAIN_BATCH_SIZE, # '16'
        'eval_batch_size': EVAL_BATCH_SIZE, # '64'
        'warmup_steps': WARMUP_STEPS, # 500
        'learning_rate': LEARNING_RATE # 1e-5 to 5e-5
    }
    
    input_data_config = [
        {
            'ChannelName': 'train',
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': 's3://sagemaker-research-methodology-extraction/01_data/03_core/unified_works_train/',
                    'S3DataDistributionType': 'FullyReplicated'
                }
            },
            'InputMode': 'File'
        }
    ]
    
    try:
        resp = SAGEMAKER_CLIENT.create_training_job(
            TrainingJobName=JOB_NAME,
            RoleArn=EXECUTION_ROLE,
            AlgorithmSpecification={
                'TrainingImage': image_uri,
                'TrainingInputMode': 'File'
            },
            HyperParameters=hyperparameters,
            InputDataConfig=input_data_config,
            OutputDataConfig={
                'S3OutputPath': f's3://{config.DEFAULT_S3_BUCKET_NAME}/03_training_output/{JOB_NAME}'
            },
            ResourceConfig={
                'InstanceType': INSTANCE_TYPE,
                'InstanceCount': 1,
                'VolumeSizeInGB': VOLUME_SIZE_GB
            },
            StoppingCondition={'MaxRuntimeInSeconds': MAX_RUNTIME_S},
            Environment=ENV_VARS,
            EnableManagedSpotTraining=False
        )
        print('Training job created:', JOB_NAME)
    except ClientError as e:
        print('create_training_job failed:')
        print(e.response.get('Error', e))
        raise
    return JOB_NAME


def get_available_training_quotas():
    instance_capacity = {}
    instances = [
        'ml.g6.xlarge',
        'ml.g6.2xlarge',
        'ml.g6.4xlarge',
        'ml.g6.8xlarge',
        'ml.g6.12xlarge',
        'ml.g6.16xlarge',
        'ml.g6.24xlarge',
        'ml.g6.48xlarge'
    ]

    quota_client = boto3.client(
        service_name='service-quotas',
        region_name=config.AWS_REGION
    )

    sagemaker_client = boto3.client(
        service_name='sagemaker',
        region_name=config.AWS_REGION
    )

    first_page = True
    page = {}
    services = []
    while first_page or 'NextToken' in page:
        first_page = False
        if 'NextToken' in page:
            page = quota_client.list_services(MaxResults=100, NextToken=page['NextToken'])
        else:
            page = quota_client.list_services(MaxResults=100)
        services = services + page['Services']

    first_page = True
    page = {}
    quotas = {}
    while first_page or 'NextToken' in page:
        first_page = False
        if 'NextToken' in page:
            page = quota_client.list_service_quotas(ServiceCode='sagemaker', MaxResults=100, NextToken=page['NextToken'])
        else:
            page = quota_client.list_service_quotas(ServiceCode='sagemaker', MaxResults=100)
        for quota in page['Quotas']:
            if quota['QuotaName'] not in quotas:
                quotas[quota['QuotaName']] = quota['Value']
            else:
                raise ValueError('Quota name already exists')

    for instance in instances:
        quota = int(quotas[f'{instance} for training job usage'])
        instance_capacity[instance] = {'quota': quota, 'usage': 0, 'available': quota}

    first_page = True
    page = {}
    while first_page or 'NextToken' in page:
        first_page = False
        if 'NextToken' in page:
            page = sagemaker_client.list_training_jobs(NextToken=page['NextToken'])
        else:
            page = sagemaker_client.list_training_jobs()
        for training_job_summary in page['TrainingJobSummaries']:
            if training_job_summary['TrainingJobStatus'] in ('InProgress', 'Stopping'):
                training_job = sagemaker_client.describe_training_job(TrainingJobName=training_job_summary['TrainingJobName'])
                instance = training_job['ResourceConfig']['InstanceType']
                instance_capacity[instance]['usage'] += 1
                instance_capacity[instance]['available'] -= 1

    return instance_capacity




# If loading this module after kernel start with the usual way:
# # import os
# # import sys
# # from importlib import reload
# # sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..', '01_modules')))
# # import helper_utils
# # reload(helper_utils)
# # from helper_utils import get_secret
# there should be two printed version numbers printed in the loader notebook, 
# while on subsequent cell reruns, there should be only one version printed
print(f'utils.py loaded: v{version_test()}')