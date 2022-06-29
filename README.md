# MGMT 590 Big Data in the Cloud (Krannert, Purdue University)

This repository contains code for labs and demos as well as any resources needed to get started with the class assignments and projects.
It is assumed that you will be working primarily from Google Cloud, meaning that the scripts and instructions assume that you have an account in Google Cloud with a project defined and you have linked the credits Google has graciously provided to your project.

## Directories
The repository is organized into the following folders:
* notebooks: Jupyter notebooks
* sh: home for BASH shell scripts 
* data: where the shell scripts download data to
* schemas/bigQuery: table schemas for use with Google Big Query
* schemas/pubSub: data schemas for use with Google Cloud Pub/Sub
* python: Python code

## Obtaining the Class Resources
The latest version of the repository of code and notes is provided via GitHub at [bigDataNCloud/classResources at GitHub](https://github.com/bigDataNCloud/classResources).
By using Cloud Shell in Google Cloud, the git utility is already installed and configured.
Git is a utility that makes it easy to work collaboratively on coding projects.
You can obtain a copy of the repository (called "cloning") by doing the following from within Cloud Shell:
```
> git clone https://github.com/bigDataNCloud/classResources
```
This will create a subdirectory named classResources with a copy of all of the stuff you see in this repository. 
Let's refer to the subdirectory "classResources" as BIG_DATA_HOME.

## Updating Your Local Copy of Class Resources
As new files are added or updates are made to the repository, you can synchronize your local copy by going to BIG_DATA_HOME and executing:
```
> git pull
```

__NOTE:__ _If you have updating files in your copy of the repository, you will want to commit changes using git before pulling in new edits._
```
> git commit -m "Some explanation for why I changed things."
```
Typically, your changes will merge without any conflicts unless you are working on the same lines of code that are being updated in GitHub.

BIG_DATA_HOME should look like the directory structure shown in GitHub.

```
❯ ls -l classResources
total 56
-rw-r--r-- 1 krannert_big_data_n_cloud krannert_big_data_n_cloud 35149 Jun 25 19:25 LICENSE
drwxr-xr-x 2 krannert_big_data_n_cloud krannert_big_data_n_cloud  4096 Jun 25 19:25 notebooks
drwxr-xr-x 2 krannert_big_data_n_cloud krannert_big_data_n_cloud  4096 Jun 25 19:25 python
-rw-r--r-- 1 krannert_big_data_n_cloud krannert_big_data_n_cloud  6451 Jun 25 19:25 README.md
drwxr-xr-x 2 krannert_big_data_n_cloud krannert_big_data_n_cloud  4096 Jun 25 19:44 sh
```

## Setting up a Python Environment
To be able to use the provided Python code, we will set up a Python virtual environment and install the python dependencies contained in BIG_DATA_HOME/python/requirements.txt. 

, activate the python environment and install all of the dependencies the code requires.

1. Use virtualenv to set up a virtual python environment named "bigdata_venv" from within BIG_DATA_HOME, which creates a Python sandbox where we will install the Python libraries needed to run the code.
```
> virtualenv -p python3 bigdata_venv
created virtual environment CPython3.9.2.final.0-64 in 1803ms
  creator CPython3Posix(dest=/home/krannert_big_data_n_cloud/classResources/bigdata_venv, clear=False, no_vcs_ignore=False, global=False)
  seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/home/krannert_big_data_n_cloud/.local/share/virtualenv)
    added seed packages: pip==22.0.4, setuptools==62.1.0, wheel==0.37.1
  activators BashActivator,CShellActivator,FishActivator,NushellActivator,PowerShellActivator,PythonActivator
```
2. _Activate_ the Python installation in the newly created sandbox so that all of the libraries we install will be installed locally inside the sandbox.
```
> source bigdata_venv/bin/activate
```
3. Use Python's package manager, pip, to install all of the libraries in the requirements.txt file.
```
> pip install -r python/requirements.txt
Collecting anyio==3.3.0
  Downloading anyio-3.3.0-py3-none-any.whl (77 kB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 77.9/77.9 KB 3.5 MB/s eta 0:00:00
...
Installing collected packages: ...
```

The requirements.txt file contains a list of all the libraries that the code in this repository depends on. If the command with "pip install -r" fails, you will need to resolve the issue, or else some of the code may not run because libraries will be missing. (If it gives a warning, such as to tell you that there is a newer version of pip, you can continue regardless.)

You can _enter_ and _exit_ the virtual Python environment. To enter, do as we did above:
```
> source bigdata_env/bin/activate
```
Once entered, all of the Python libraries you installed will be available.

To exit, execute the deactivate command.
```
> deactivate
```

## Setting up Jupyter Lab
The BIG_DATA_HOME/notebooks directory has notebooks to use with Jupyter. You can have Jupyter recognize Python code and execute it within a Python virtual environment that you set up (if you install the code on your local machine.)

__NOTE:__ This does not run code in Google Cloud. You can still view the content of the notebooks without actually having to run the code snippets in the notebooks. The following is just for reference if you have Python installed and set up on your local machine and the code downloaded.

To inform Jupyter of your own virtual environment, execute the following (assuming your virtual environment is named "local_venv"):
```
> python -m ipykernel install --name=local_venv
Installed kernelspec bigdata_venv in /usr/local/share/jupyter/kernels/local_venv
```

Then, you can run the script in BIG_DATA_HOME/sh that starts Jupyter on your local machine:
```
> ./sh/runJupyter.sh
```

This should open up a page in your web browser showing Jupyter Lab.
