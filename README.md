# MGMT 590 Big Data in the Cloud (Krannert, Purdue University)

This repository contains code for labs and demos as well as any resources needed to get started with the class assignments and project.
You can download the entire set of resources as a zip file or use git to grab the data. 

## Directories
* notebooks: Jupyter notebooks
* sh: home for BASH shell scripts 
* data: where the shell scripts download data to
* bigQuerySchemas: table schemas for use with Google Big Query
* pubSubSchemas: data schemas for use with Google Cloud Pub/Sub
* python: Python code

## Obtaining the Class Resources
You can download the latest version of the repository as a zip file or set up Git on your local machine to synchronize a local repository with the one in GitHub. If you are not familiar with Git, it may be too much to learn all about Git in a short time and you may feel more comfortable going the manual route, but Git is not difficult when you are not contributing to or changing code in the repository.

### Manually Installing by Downloading a Compressed Snapshot
1. Download the code as a zip file and unzip the file. This will create a folder named classResources-main with all the contents of the zip file expanded.
1. Move the expanded folder to where you want to install the code, such as to your home directory.
1. Change directory to be within the code directory. (In my case, I am in $HOME/classResources.) Let's call this folder BIG_DATA_HOME.

### Setting up Git
You can use git to keep a local copy of the repository in sync with changes in GitHub. You don't need to set up and learn Git to access the resources, though it makes it simpler to synchronize your local copy if there are any changes uploaded to the repository in GitHub.
If you go this route, you will need to install git on your local machine (or use Google Cloud resources.) GitHub has some tools that can help simplify this process.
1. Once git is installed, you can clone the repository that is in GitHub. Choose where you want to install the classResources and execute the following:
> git clone https://github.com/bigDataNCloud/classResources

This will create a subdirectory named classResources with a copy of all of the stuff you see in this repository. Let's refer to the subdirectory "classResources" as BIG_DATA_HOME.

1. If new files or updates are made to the GitHub repository, you can update your local repository by going to BIG_DATA_HOME and executing:
> git pull

There are a myriad of issues that can happen when you update with git if you have made changes to the code you downloaded and the same files have been changed in the GitHub repository. Commit your changes (which commits them to your local repository) and then try updating. Beyond this, there are tons (probably too much?) of help in the internet.

BIG_DATA_HOME should look like the directory structure shown in GitHub.

## Setting up a Python Environment
To be able to use the Python code within the subdirs, I suggest that you set up a Python virtual environment and install the python dependencies within requirements.txt in the outermost directory. 

Set up a virtual python environment named "bigdata_venv" from within BIG_DATA_HOME, activate the python environment and install all of the dependencies the code requires.
1. virtualenv -p python3 bigdata_venv
1. source bigdata_venv/bin/activate
1. pip install -r requirements.txt

The requirements.txt file contains a list of all the libraries that the code in this repository depends on. If the last command with "pip install -r" fails, you will need to resolve the issue or else some of the code may not run because libraries will be missing. (If it gives a warning, such as to tell you that there is a newer version of pip, you can continue regardless.)

## 
In the twitterWithOpenSky_assignement3, there is a Jupyter notebook. You can run the Jupyter notebook and have it recognize the code in the subdirectories (actually, all of those named "python").
To inform Jupyter of your own virtual environment, execute the following (assuming your virtual environment is named "bigdata_venv") from within BIG_DATA_HOME:
> python -m ipykernel install --name=bigdata_venv

This command should state something like:
> Installed kernelspec bigdata_venv in /usr/local/share/jupyter/kernels/bigdata_venv

Then you can run the script (also in BIG_DATA_HOME) that starts Jupyter on your local machine:
> ./runJupyter.sh

This should open up a page in your web browser showing Jupyter Lab.
