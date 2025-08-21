import json
import boto3
import time
from botocore.exceptions import ClientError
from typing import Union
from datetime import datetime, timezone


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