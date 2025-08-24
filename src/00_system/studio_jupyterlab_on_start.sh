#!/bin/bash

# stop the script execution on error
set -eux

# Source: https://github.com/aws-samples/sagemaker-studio-lifecycle-config-examples/blob/main/scripts/set-git-config/on-jupyter-server-start.sh
# PARAMETERS
YOUR_USER_NAME="ka-steve"
YOUR_EMAIL_ADDRESS="attila.steve.kopias@gmail.com"

git config --global user.name "$YOUR_USER_NAME"
git config --global user.email "$YOUR_EMAIL_ADDRESS"

#sets the environment channels to conda-forge package repositories
conda config --add channels conda-forge
conda config --set channel_priority strict

conda create -n python_311 python=3.11.13 -y
source activate python_311

# Install Pytorch since the pip version is incompatible with sagemaker due to its +cu123 version suffixes
# conda install torch==2.6.0 torchvision==0.21.0 torchaudio==2.6.0 --index-url https://download.pytorch.org/whl/cu126 -y

# python -m pip install "numpy<2.3.0" ipykernel jupyterlab matplotlib
# python -m ipykernel install --user --name python_311 --display-name python_311
python -m pip install -r /home/sagemaker-user/research_methodology_extraction/requirements.txt
