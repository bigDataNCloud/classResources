from argparse import ArgumentParser

import functions_framework
import os
import json
import logging
from datetime import datetime,date
from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient

logging.basicConfig(
  format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
  datefmt="%Y-%m-%d %H:%M:%S")
_logger=logging.getLogger(__name__)

_storageClient=None
_columns=[
  'date',
  'location',
  'total_vaccinations',
  'total_distributed', 'people_vaccinated', 'people_fully_vaccinated_per_hundred', 'total_vaccinations_per_hundred', 'people_fully_vaccinated', 'people_vaccinated_per_hundred', 'distributed_per_hundred', 'daily_vaccinations_raw', 'daily_vaccinations', 'daily_vaccinations_per_million', 'share_doses_used', 'total_boosters', 'total_boosters_per_hundred']

# Entry point for a Cloud Function is called "entry".
# Rename this file to "main.py" when you upload it to create a Cloud Function.
# Rename the requirements_vaccinations.txt to requirements.txt when you upload it to create a Cloud Function.
# Trigger a cloud function with the following test:
{
  "bucket":"batch-data-cap",
  "path":"vaccine-data-test",
  "projectId":"prof-big-data",
  "addTimestamp":"true",
  "topic":"vaccines-topic",
  "pubsub":"true",
  "inputPath":"covid_vaccinations_us_state_vaccinations_aug.txt"
}

def convertType(item):
  '''
  This utility will guess what the type is of the given string and convert it into a Python primitive of str, int, float.
  Dates and times are not converted into Python date and datetime since json.dumps is not able to translate these.
  Args:
    item: a string with either characters, a number, a date, or a timestamp.
  Returns:
    returns the item as a Python primitive.
  '''
  # Use trial and error to convert the item. Return whatever does not throw an exception.
  try:
    return int(item)
  except:
    pass
  try:
    return float(item)
  except:
    pass
  if '/' in item:
    try:
      itemParts=item.split('/')
      return date(int(itemParts[2]),int(itemParts[0]),int(itemParts[1])).strftime('20%y-%m-%d')
    except:
      pass
  return item  # Return as a string if all the other attempts through exceptions.

def convertToJson(csvData, columns, delimiter=None):
  '''
  This is a simple method to convert csv data into a JSON object.
  You can use this when you publish data as JSON when it is originally as CSV.
  Args:
    csvData: a string with comma delimited values.
    columns: the column names as a list.
  Returns:
     returns the json form of the data as a string.
  '''
  # split: Split out the columns of the data by commas.
  # map: Convert the data to Python primitives.
  # zip: Collate the columns with the data.
  # dict: Create a Python dict of the data.
  # json.dumps: Convert the Python dict into a JSON string.
  values=csvData.split(',' if delimiter is None else delimiter)
  cleanValues=map(lambda value:value.replace('\r',''),values)
  # split: Split out the columns of the data by commas.
  # map: Convert the data to Python primitives.
  # zip: Collate the columns with the data.
  # dict: Create a Python dict of the data.
  # json.dumps: Convert the Python dict into a JSON string.
  return json.dumps(dict(filter(lambda column_value:type(column_value[1])!=str or len(column_value[1])>0,zip(columns, map(convertType,cleanValues)))))

def _getStorageClient(bucket):
  '''
  Args:
    bucket:
  Returns: returns an existing connection to GCS or else creates a new connection to GCS.
  '''
  global _storageClient
  if _storageClient is None:
    # This is the first time we are hitting storage, so open a new connection.
    _storageClient=storage.Client().bucket(bucket)
  if not _storageClient.exists(): raise Exception('Cannot access bucket '+bucket)
  return _storageClient

def _store(bucket, path, data):
  '''
  An action that stores the data in the bucket at the given path.
  Args:
    bucket:
    path:
    data:
  Returns:
  '''
  try:
    return _getStorageClient(bucket).blob(path).upload_from_string(data)
  except:
    _logger.error('Cannot write to '+path+' in '+bucket, exc_info=True, stack_info=True)

def _publish(projectId, topic, row, additional=None):
  '''
  An action that writes the data to the given topic.
  Args:
    projectId:
    topic:
    row: a string consisting of lines to publish as separate messages. The first line is assumed to be a header.
    additional: any additional text to add to the end of the line. If data is comma-delimited, then don't forget to add a comma to addtional,
                such as _publish(..., additional=",SYMBOL" )
  '''
  try:
    pubsubClient=PublisherClient()
    topicPath='projects/'+projectId+'/topics/'+topic
    publishingFutures=[]  # Will collect all the future publish calls in this list.
    # Don't publish a message that only has empty entries or is an empty line.
    if len(row.replace('\t', '').strip())>0:
      if additional is not None: row+=additional
      # Convert row into JSON.
      jsonRow=convertToJson(row, _columns, delimiter='\t')
      publishingFutures.append(pubsubClient.publish(topicPath, jsonRow.encode()))  # Encode the data as bytes.
    for publishing in publishingFutures:
      publishing.result()  # Calling the result() method will cause the future command to actually execute if it hasn't already done so.
  except:
    _logger.error('Cannot publish to '+topic, exc_info=True, stack_info=True)

def _getMessageJSON(request):
  '''
  A request that triggers a Cloud Function can be formatted in a variety of ways. (Some of these may now be legacy.)
  This method uses some trial and error to search for the message within the request.
  :request: passed into a Cloud Function when it is triggered.
  :return: returns an object parsed from the message if the message can be identified, otherwise None.
  '''
  request_json=request.get_json()
  message=None
  
  if request.args is not None:
    _logger.debug('request is '+str(request)+' with args '+str(request.args))
    if 'message' in request.args: message=request.args.get('message')
    
    if any(map(lambda param:param in request.args, ['bucket', 'path', 'topic', 'projectId'])):
      # request.args holds the fields we are expecting to exist in the message.
      message=request.args
  if message is None and request_json is not None:
    # If message remains unset (None) then assuming that the request_json holds the contents of the message we are looking for.
    _logger.debug('request_json is '+str(request_json))
    if 'message' in request_json:
      message=request_json['message']
    else:
      message=request_json
  
  if message is None:
    _logger.warning('message is empty. request='+str(request)+' request_json='+str(request_json))
    message='{}'
  
  if type(message)==str:
    # If message type is str and not dict, attempt to parse as a JSON string.
    try:
      messageJSON=json.loads(message)
    except:
      try:
        _logger.error('ERROR Cannot parse provided message '+str(message), exc_info=True, stack_info=True)
      except:
        pass
      messageJSON={}
  else:
    # Else, assuming messageJSON was decoded from a JSON object.
    messageJSON=message
  return messageJSON

def _parse(row,action):
  '''
  Query the API for the data on the given stock and act on it.
  Args:
    stock:
    period:
    interval:
    action: a function that takes the data returned by the API and acts on it.
  Returns:
  '''
  return action(row)

def parseAll(inputPath,bucket=None,path=None,projectId=None,topic=None,store=True,publish=True):
  rowNum=0
  dataFile=_getStorageClient(bucket).blob(inputPath)
  if dataFile.exists():
    dataRows=dataFile.download_as_bytes().decode('utf-8').split('\n')
    for row in dataRows:
      try:
        actions=[]
        if store: actions.append(
          lambda data:_store(bucket, '{path}/realtimeData.csv'.format(path=path), data))
        if publish: actions.append(lambda data:_publish(projectId, topic, data))
        for action in actions:
          _parse(row, action)
        rowNum+=1
      except:
        _logger.error('Cannot parse row '+str(rowNum))
  else:
    _logger.error('Cannot read data from '+inputPath+' in bucket '+bucket)
  return rowNum

@functions_framework.http
def entry(request):
  '''
  Args:
    request: the request is passed into the cloud function and message will have the JSON that the funciton is triggered with.
  '''
  _logger.setLevel(10)
  message=_getMessageJSON(request)
  
  debug=message.get('debug', 10)
  if debug==0:
    debug=None
  else:
    _logger.setLevel(debug)
  
  projectId=message.get('projectId', os.environ.get('GOOGLE_CLOUD_PROJECT', 'no_project'))
  bucket=message.get('bucket', projectId+'_data')
  path=message.get('path', 'data')
  addTimestamp=message.get('addTimestamp', None)
  topic=message.get('topic', None)
  store=message.get('storage', False)
  publish=message.get('pubsub', False)
  inputPath=message.get('inputPath','covid/vaccinations/us_state_vaccinations_aug.txt')
  if not publish and not store: store=True
  if addTimestamp=='true':
    # Append a timestamp to the path so that we don't overwrite an existing set of files.
    path+='/timestamp='+datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
  numParsed=parseAll(inputPath, bucket=bucket, path=path, projectId=projectId, topic=topic,
                     store=store, publish=publish)
  return 'Completed parsing '+str(numParsed)+' rows.'

if __name__=='__main__':
  '''
  This segment of code will run when you execute the code from the command line (i.e., not when it is run from within a
  cloud function.) I use it to test the code before wrapping it in a cloud function.

  To run it from the command line, you will need to point python to the directory that has the code. You can set the
  PYTHONPATH at the beginning of the statement to execute, like so:
      PYTHONPATH=~/classResources/python python ~/classResources/python/api/stocks/yahooFinance.py -h
  '''
  parser=ArgumentParser()
  parser.add_argument('-bucket', default=None)
  parser.add_argument('-path', default='data')
  parser.add_argument('-projectId', default=None)
  parser.add_argument('-topic', default=None)
  parser.add_argument('-storage', action='store_true')
  parser.add_argument('-publish', action='store_true')
  parser.add_argument('-addTimestamp', action='store_true')
  parser.add_argument('-inputPath', default='covid/vaccinations/us_state_vaccinations_aug.txt')
  args=parser.parse_args()
  projectId=os.environ.get('GOOGLE_CLOUD_PROJECT', 'no_project') if args.projectId is None else args.projectId
  bucket=projectId+'_data' if args.bucket is None else args.bucket
  addTimestamp=args.addTimestamp
  topic=args.topic
  path=args.path
  inputPath=args.inputPath
  if args.addTimestamp:
    # Append a timestamp to the path so that we don't overwrite an existing set of files.
    path+='/timestamp='+datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
  parseAll(args.inputPath,bucket=args.bucket, path=args.path, projectId=projectId,
           topic=args.topic,
           store=args.storage, publish=args.publish)