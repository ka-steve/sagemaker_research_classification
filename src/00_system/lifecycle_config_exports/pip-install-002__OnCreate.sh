#!/bin/bash

set -e

echo "installing jupytext plugin into the jupyter(lab) environment"

sudo -u ec2-user -i <<EOF

# to install jupyter plugins, you need to select the jupyter environment
source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv

# install jupytext
pip install jupytext --upgrade
# As of Feb 2021, Sagemaker is using jupyterlab v 1.x for which Jupytext recommends the following version.
jupyter labextension install jupyterlab-jupytext@1.1.1

# allow jupyter to open and save notebooks as text files.
echo c.NotebookApp.contents_manager_class="jupytext.TextFileContentsManager" >> /home/ec2-user/.jupyter/jupyter_notebook_config.py

# restart jupyter to let the changes take effect.
sudo initctl restart jupyter-server --no-wait

EOF