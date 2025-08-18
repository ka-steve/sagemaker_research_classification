#!/bin/bash

set -e

# OVERVIEW
# This script installs a single pip package in a single SageMaker conda environments.

sudo -u ec2-user -i <<'EOF'

# PARAMETERS
ENVIRONMENT=python3

source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

# pip install "jupytext" "sagemaker==2.250.0" "transformers==4.55.2" "datasets==4.0.0" "accelerate==1.10.0"
pip install /home/ec2-user/SageMaker/research_methodology_extraction

# source /home/ec2-user/anaconda3/bin/deactivate
conda deactivate


# to install jupyter plugins, you need to select the jupyter environment
source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv

# install jupytext
pip install jupytext --upgrade
# As of Feb 2021, Sagemaker is using jupyterlab v 1.x for which Jupytext recommends the following version.
jupyter labextension install jupyterlab-jupytext #@1.1.1

# allow jupyter to open and save notebooks as text files.
echo c.NotebookApp.contents_manager_class="jupytext.TextFileContentsManager" >> /home/ec2-user/.jupyter/jupyter_notebook_config.py

EOF