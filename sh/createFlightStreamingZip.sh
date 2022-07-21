#!/bin/bash

FUNCTION=$1
if [ ! -z ${FUNCTION} ]
then
  echo "Must provide the function name (which must also match the requirements-FUNCTION.txt and main_FUNCTION.py file names.)"
fi
echo "Creating a Cloud Function zip for ${FUNCTION}"

ORIG_PWD=`pwd`
# Navigate to the root folder of the code.
cd $HOME
for codeHome in `find . -name "classResources"`
do
  CODE_HOME=${codeHome}
  break
done
echo "CODE_HOME=${CODE_HOME}"
if [ -x ${CODE_HOME} ]
then
  rm -rf /tmp/${FUNCTION}_zip
  mkdir /tmp/${FUNCTION}_zip
  cd /tmp/${FUNCTION}_zip
  cp ${HOME}/${CODE_HOME}/python/requirements_${FUNCTION}.txt requirements.txt
  cp ${HOME}/${CODE_HOME}/python/main_${FUNCTION}.py main.py
  mkdir flight
  cp -r ${HOME}/${CODE_HOME}/python/flight/stream flight
  # Create a folder that contains all the files needed for the Cloud Function:
  #   requirements... -- lists the libraries and versions the code depends on.
  #   flightStreamingRunner.py -- the entry point for the Cloud Function to call when triggered.
  #   flight/stream/* -- the code
  zip -r ../${FUNCTION}.zip .
  #   outputs a zip file in ${CODE_HOME}.
  gsutil cp ../${FUNCTION}.zip gs://${GOOGLE_CLOUD_PROJECT}_data/function/
  echo "Uploaded ${FUNCTION}.zip to the function directory in the ${GOOGLE_CLOUD_PROJECT}_data bucket."
else
  echo "Cannot locate the classResources directory."
fi
cd $ORIG_PWD