import json
import boto3
import time
import os
import awswrangler as wr
from botocore.exceptions import ClientError
from typing import Union, List, Optional
from datetime import datetime, timezone
import config


def version_test()->str:
    # Helper util to confirm that the last version of the helper_utils.py is loaded
    version = '0.2.12'
    # print(version)
    return version

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
        s3_output=s3_parent_target_path
    )

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