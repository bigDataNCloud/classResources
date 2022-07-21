#!/bin/bash
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
  rm -rf /tmp/flightStreamingFunctionZip
  mkdir /tmp/flightStreamingFunctionZip
  cd /tmp/flightStreamingFunctionZip
  cp ${CODE_HOME}/python/requirements_flightStreamingRunner.txt requirements.txt
  cp ${CODE_HOME}/python/flightStreamingRunner.py .
  mkdir flight
  cp -r ${CODE_HOME}/python/flight/stream flight
  # Create a folder that contains all the files needed for the Cloud Function:
  #   requirements... -- lists the libraries and versions the code depends on.
  #   flightStreamingRunner.py -- the entry point for the Cloud Function to call when triggered.
  #   flight/stream/* -- the code
  zip -r ../flightStreamingFunction.zip .
  #   outputs a zip file in ${CODE_HOME}.
  gsutil cp ../flightStreamingFunction.zip gs://${GOOGLE_CLOUD_PROJECT}_data/function/
  echo "Uploaded zip file to the function directory in the ${GOOGLE_CLOUD_PROJECT}_data bucket."
else
  echo "Cannot locate the classResources directory."
fi
cd $ORIG_PWD