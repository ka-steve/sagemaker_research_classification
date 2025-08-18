#!/bin/bash

set -e

echo "XXXXXX create_notebook START XXXXXXXXXXXXXXXXXX"

sudo -u ec2-user -i <<EOF

echo ":::::: create_notebook SUDO START ::::::::::::::::::"
echo ":: source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv"
source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv

echo ":: pip install jupytext --upgrade"
pip install jupytext --upgrade


echo ":: sudo systemctl restart jupyter-server"
sudo systemctl restart jupyter-server
echo "XXXXXX create_notebook SUDO END XXXXXXXXXXXXXXXXXX"

EOF

echo ":::::: create_notebook END ::::::::::::::::::::::::"