#!/bin/bash
REPO_PATH="/home/rob/initiative"
VENV_PATH="/home/rob/initiative/venv"

cd $REPO_PATH
source $VENV_PATH/bin/activate
python setup.py >> $REPO_PATH/setup.log 2>&1
