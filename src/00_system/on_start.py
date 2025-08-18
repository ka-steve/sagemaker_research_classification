# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: conda_pytorch_p310
#     language: python
#     name: conda_pytorch_p310
# ---

# %% [markdown]
# ### Startup  
#   
# This script is supposed to be executed when a SageMaker AI Notebook Instance is started. It can contain multiple functionalities, but currently only contains one: it saves the lifecycle configurations (that were entered manually on the AWS console) into files to make sure they can be version controlled. It is Ouroboros solution, since it needs to be called from the lifecycle scripts it exports. Using Terraform would be much more straightforward, but that is not within the scope of the current project.  
#
# The on_start.ipynb Notebook file is linked through Jupytext to on_start.py so that changes in either of those automatically update the other. This allows for quick development in a notebook, while executing it later as a Python script.

# %% [markdown]
# #### 1. Exporting Notebook Instance Lifecycle Configurations

# %%
import json
import boto3
import base64
import os
from pathlib import Path

# %%
cwd = os.getcwd()
lifecycle_config_exports_path = os.path.join(cwd, 'lifecycle_config_exports')
Path(lifecycle_config_exports_path).mkdir(parents=True, exist_ok=True)

# %%
boto_sagemaker = boto3.client('sagemaker')

# %%
notebook_instance_lifecycle_configs_response = boto_sagemaker.list_notebook_instance_lifecycle_configs(
    SortBy = 'Name',
    SortOrder = 'Ascending',
    MaxResults = 100 # Max value, implement paging loging using 
    # response.NextToken if more than 100 Lifecycle Configurations are possible
    # See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker/client/list_notebook_instance_lifecycle_configs.html
)
for notebook_instance_lifecycle_config in notebook_instance_lifecycle_configs_response['NotebookInstanceLifecycleConfigs']:
    notebok_instance_lifecycle_config_name = notebook_instance_lifecycle_config['NotebookInstanceLifecycleConfigName']
    notebok_instance_lifecycle_config_description = boto_sagemaker.describe_notebook_instance_lifecycle_config(
        NotebookInstanceLifecycleConfigName = notebok_instance_lifecycle_config_name
    )
    for runtype in ['OnCreate', 'OnStart']:
        if runtype in notebok_instance_lifecycle_config_description:
            print(notebok_instance_lifecycle_config_name, runtype)
            lifecycle_base64 = notebok_instance_lifecycle_config_description[runtype][0]['Content']
            lifecycle_string = '\n'.join(f'{base64.b64decode(lifecycle_base64).decode("utf-8")}'.split('\\n'))
            filename = f'{notebok_instance_lifecycle_config_name}__{runtype}.sh'
            filepath = os.path.join(lifecycle_config_exports_path, filename)
            with open(filepath, "w", encoding='utf-8') as config_file:
                config_file.write(lifecycle_string)
