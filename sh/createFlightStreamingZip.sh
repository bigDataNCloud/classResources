#!/bin/bash

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
  cd ${CODE_HOME}/python
  # Create a zip file with selected files:
  #   requirements... -- lists the libraries and versions the code depends on.
  #   flightStreamingRunner.py -- the entry point for the Cloud Function to call when triggered.
  #   flight/stream/* -- the code
  #   outputs a zip file in ${CODE_HOME}.
  zip -r ../flightStreamingFunction.zip requirements_flightStreamRunner.txt flightStreamingRunner.py flight/stream
  gsutil cp ../flightStreamingFunction.zip gs://${GOOGLE_CLOUD_PROJECT}_data/function/
  echo "Uploaded zip file to the function directory in the ${GOOGLE_CLOUD_PROJECT}_data bucket."
else
  echo "Cannot locate the classResources directory."
fi