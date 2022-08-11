from argparse import ArgumentParser
import functions_framework
import os
import json
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient
import requests

examples=[
  {
     "debug":10,
     "bucket":"prof-big-data_data",
     "path":"traffic",
     "projectId":"prof-big-data",
     "key":"NO KEY GIVEN",
     "bounds":[39.95,-105.25,39.52,-104.71],
     "filters":["construction","incidents"],
     "addTimestamp":"true",
     "storage":"true",
     "topic":"traffic-topic",
     "pubsub":"true"
  }
]

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

_storageClient=None
_baseURL='http://www.mapquestapi.com/traffic/v2/incidents'

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

def _publishOneRow(pubsubClient,topicPath,row):
  cleaned=row.strip()
  if len(cleaned)>0:
    # Don't publish an empty message.
    return pubsubClient.publish(topicPath, cleaned.encode())  # Encode the data as bytes.
  return None

def _publish(projectId, topic, data, additional=None):
  '''
  An action that writes the data to the given topic.
  Args:
    projectId:
    topic:
    data:
    additional: any additional text to add to the end of the line. If data is comma-delimited, then don't forget to add a comma to addtional,
                such as _publish(..., additional=",SYMBOL" )
  Returns:
  '''
  try:
    pubsubClient=PublisherClient()
    topicPath='projects/'+projectId+'/topics/'+topic
    publishingFutures=[]  # Will collect all the future publish calls in this list.
    if type(data)==str:
      if additional is not None: data+=additional
      future=_publishOneRow(pubsubClient,topicPath,data)
      if future is not None: publishingFutures.append(future)
    elif type(data)==dict:
      data['additional']=additional
      future=_publishOneRow(pubsubClient,topicPath,json.dumps(data))
      if future is not None: publishingFutures.append(future)
    elif type(data) in [list,tuple,set]:
      for row in data:
        if additional is not None: row += additional
        future=_publishOneRow(pubsubClient,topicPath,row)
        if future is not None: publishingFutures.append(future)
    _logger.debug('Will publish '+str(len(publishingFutures))+' messages.')
    for publishing in publishingFutures:
      publishing.result()  # Calling the result() method will cause the future command to actually execute if it hasn't already done so.
  except:
    _logger.error('Cannot publish to '+topic, exc_info=True, stack_info=True)

def _getData(key,bounds,filters):
  '''
  Query the API for the data on the given stock and act on it.
  Args:
    key: mapquest key.
    bounds: a list of 2 tuples, where each tuple is (latitude,longitude). These 2 tuples specify the minimum-left and maximum-right bounds.
    filters: a list of the types of incidents to retrieve.
  Returns:
  '''
  minLat,minLong=bounds[0]
  maxLat,maxLong=bounds[1]
  response=requests.get('{url}?key={key}&boundingBox={minLat},{minLong},{maxLat},{maxLong}&filters={filters}'.format(
    url=_baseURL,
    key=key,
    minLat=minLat,
    minLong=minLong,
    maxLat=maxLat,
    maxLong=maxLong,
    filters=','.join(filters)
  )).json()
  if 'incidents' in response:
    return response['incidents']
  return None

def _parse(data,action):
  '''
  Query the API for the data on the given stock and act on it.
  Args:
    data: a string containing the data to act on. This string can be multiple lines long.
    action: a function that takes the data returned by the API and acts on it.
  Returns:
  '''
  return action(data)

def parseAll(key,bounds,filters,bucket=None,path=None,projectId=None,topic=None,store=True,publish=True):
  '''
  
  Args:
    key: your mapquest key.
    bounds: a list of 2 tuples, where each tuple is (lat,long). These are the min and max corners of the bounding box.
    filters: a list of the types of incidents to include, such as ["construction","incidents"]
    bucket:
    path:
    projectId:
    topic:
    store:
    publish:
  Returns: returns the number of items published and written.

  '''
  num=0
  incidents=_getData(key,bounds,filters)
  if incidents is not None:
    if publish:
      action=lambda data:_publish(projectId, topic, data)
      for datum in incidents:
        _parse(datum,action)
        num+=1
    if storage:
      action=lambda data: _store(bucket,'{path}/incidents.json'.format(path=path),data) # Store the chunk of data as one file. This will be in JSONL format for BigQuery.
      allIncidents='\n'.join(map(lambda datum:json.dumps(datum),incidents))
      _parse(allIncidents,action)
      num+=1
  return num

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
  path=message.get('path', 'traffic')
  key=message.get('key', None)
  if key is None: raise Exception('Must provide a mapquest key.')
  givenBounds=message.get('bounds', [39.95,-105.25,39.52,-104.71])
  # Convert the list of bounds into a list of 2 tuples of (lat,long).
  bounds=((givenBounds[0],givenBounds[1]),(givenBounds[2],givenBounds[3]))
  filters=message.get('filters',['construction','incidents'])
  addTimestamp=message.get('addTimestamp', None)
  topic=message.get('topic', None)
  store=message.get('storage',False)
  publish=message.get('pubsub',False)
  if addTimestamp=='true':
    # Append a timestamp to the path so that we don't overwrite an existing set of files.
    path+='/timestamp='+datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
  _logger.info('Will query for '+','.join(filters))
  if storage: _logger.info('Writing to '+path+' in bucket '+bucket)
  if publish: _logger.info('Publishing to topic '+topic)
  num=parseAll(key,bounds,filters,bucket=bucket, path=path, projectId=projectId, topic=topic,
                     store=store, publish=publish)
  return 'Completed parsing. Wrote '+str(num)+' to '+(' storage' if store else '')+(' pub/sub' if publish else '')

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
  parser.add_argument('-path', default='traffic')
  parser.add_argument('-projectId', default=None)
  parser.add_argument('-key')
  parser.add_argument('-bounds',nargs=4,default=[39.95,-105.25,39.52,-104.71],type=float)
  parser.add_argument('-filters',nargs='+',default=['construction','incidents'])
  parser.add_argument('-topic',default=None)
  parser.add_argument('-storage',action='store_true')
  parser.add_argument('-pubsub',action='store_true')
  parser.add_argument('-addTimestamp',action='store_true')
  args=parser.parse_args()
  projectId=os.environ.get('GOOGLE_CLOUD_PROJECT', 'no_project') if args.projectId is None else args.projectId
  bucket=projectId+'_data' if args.bucket is None else args.bucket
  key=args.key
  filters=args.filters
  givenBounds=args.bounds
  bounds=((givenBounds[0], givenBounds[1]), (givenBounds[2], givenBounds[3]))
  path=args.path
  topic=args.topic
  store=False if bucket is None else args.storage
  publish=False if topic is None else args.pubsub
  if args.addTimestamp:
    # Append a timestamp to the path so that we don't overwrite an existing set of files.
    path+='/timestamp='+datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
  num=parseAll(key, bounds, filters, bucket=bucket, path=path, projectId=projectId, topic=topic,
               store=store, publish=publish)
