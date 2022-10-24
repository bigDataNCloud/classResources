import logging
import json
import os
from argparse import ArgumentParser
from hashlib import sha256

from google.cloud.exceptions import Forbidden
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud import storage
from google.oauth2 import service_account

# This file has code that accesses a URL and does basic parsing of the response.
# When deployed as a Cloud Function, the entry point would be cloudFunctionMain.

from requests import Session

logging.basicConfig(
  format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
  datefmt="%Y-%m-%d %H:%M:%S")
_logger=logging.getLogger(__name__)

_expectedFieldsInFunctionCall=[]  # Fields to send as parameters to the REST API.

class RequestTemplate(object):
  '''
  Mimics a request used to trigger a Cloud Function. Instances of this class are filled with properties and passed to the
  parser when running from the command-line.
  '''
  args=None
  
  def __init__(self,
               query='', limit=None, debug=False, separateLines=True,
               projectId='', topic='',
               bucket='', path='', storage=False, pubsub=False):
    if query is not None:
      if type(query)==str:
        if not query.startswith('"'): query='"'+query+'"'
      else:
        query=json.dumps(query)
    else:
      query=''
    message={'storage':storage, 'pubsub':pubsub,
             'query':query, 'limit':limit if limit is not None else '',
             'projectId':projectId,
             'topic':topic if topic is not None else '',
             'bucket':bucket if bucket is not None else '',
             'path':path if path is not None else ''}
    if separateLines: message['separateLines']=True
    self.args={'message':json.dumps(message)}
  
  def get_json(self):
    return json.loads(self.args['message'])

def _getMessageJSON(request):
  '''
  A request that triggers a Cloud Function can be formatted in a variety of ways. (Some of these may now be legacy.)
  This method uses some trial and error to search for the message within the request.
  :request: passed into a Cloud Function when it is triggered.
  :return: returns an object parsed from the message if the message can be identified, otherwise None.
  '''
  request_json=request.get_json(force=True)
  message=None
  if request.args is not None:
    _logger.info('request is '+str(request)+' with args '+str(request.args))
    if 'message' in request.args:
      message=request.args.get('message')
    elif any(map(lambda field:field in request.args, _expectedFieldsInFunctionCall)):
      message=request.args
  if message is None and request_json is not None:
    _logger.debug('request_json is '+str(request_json))
    if 'message' in request_json:
      message=request_json['message']
    elif any(map(lambda field:field in request_json, _expectedFieldsInFunctionCall)):
      message=request_json
  
  if message is None:
    print('message is empty. request='+str(request)+' request_json='+str(request_json))
    # Use a default message.
    message='{"start":1,"limit":50,"convert":"USD"}'
  
  if type(message)==str:
    try:
      messageJSON=json.loads(message)
    except:
      _logger.error('Cannot parse arguments of the message provided to this function. message='+str(message),
                    exc_info=True, stack_info=True)
  else:
    messageJSON=message
  return messageJSON

class DataProcessor():
  @staticmethod
  def _createID(values):
    '''
    Create a unique ID given values.
    :param values: a string or a list of strings.
    :return: a string with a unique ID created from the given values.
    '''
    if type(values)==str:
      contents=values
    elif type(values)==list:
      contents=chr(1).join(map(str, values))
    else:
      contents=str(values)
    return sha256(contents.encode()).hexdigest()
  
  def __init__(self, projectId=None, topic=None, bucket=None, path=None, debug=None):
    '''

    :param projectId:
    :param topic:
    :param bucket:
    :param path:
    :param debug:
    '''
    self._bucket=bucket
    self._path=path
    self._topic=topic
    self._projectId=projectId
    if bucket is not None: _logger.debug(
      'Output will be written to {path} in {bucket}.'.format(path=self._path, bucket=self._bucket))
    if topic is not None and projectId is not None: _logger.debug(
      'Output will be published to {topic} in project {projectId}.'.format(topic=topic, projectId=projectId))
    
    self._publisher=PublisherClient() if topic is not None and projectId is not None else None
    self._bucketClient=storage.Client().bucket(bucket) if bucket is not None else None
  
  def _writeToBucket(self, data, filename=None):
    '''
    Write the given data as JSON to a file in GCS.
    :param data: any data that can be converted into a JSON formatted string.
    :param filename: name to give the file or else will create a unique key based on the contents of data.
    :return: returns the number of records written to storage.
    '''
    if data is None: return 0
    recordKey=None
    try:
      recordKey=self._createID(data) if filename is None else filename
      self._bucketClient.blob(recordKey).upload_from_string(json.dumps(data))
      return 1
    except Forbidden as fe:
      try:
        _logger.error(
          'Failed to write to GCS bucket {bucket} because access to object {objectName} is forbidden. Error code={code}, response content={response}'.format(
            bucket=self._bucket,
            objectName=recordKey,
            code=str(fe.code),
            response=fe.response.content
          ), exc_info=True, stack_info=True)
      except:
        _logger.error(
          'Failed to write to GCS bucket {bucket} because access to object {objectName} is forbidden.'.format(
            bucket=self._bucket,
            objectName=recordKey
          ), exc_info=True, stack_info=True)
    except:
      _logger.error('Failed to write to GCS bucket {bucket}, object {objectName}.'.format(
        bucket=self._bucket,
        objectName=recordKey
      ), exc_info=True, stack_info=True)
    return 0
  
  def _publish(self, data):
    attributes=None
    if type(data)==dict:
      attributes={}
      for key, value in data.items():
        if type(value) in [int, float, str, bool]:
          attributes[key]=value
    if attributes is not None and len(attributes)>0:
      # Try to include key-values of data as attributes in the published message.
      try:
        self._publisher.publish('projects/'+self._projectId+'/topics/'+self._topic,
                                data=json.dumps(data).encode("utf-8"), **attributes)
        return 1
      except:
        _logger.debug('Cannot include '+str(attributes)+' as attributes to the message.', exc_info=True)
    try:
      self._publisher.publish('projects/'+self._projectId+'/topics/'+self._topic, data=json.dumps(data).encode("utf-8"))
      return 1
    except:
      _logger.error('Cannot publish message "'+str(json.dumps(data))+'".', exc_info=True, stack_info=True)
      return 0
  
  def _parse(self, data):
    '''
    Parse data to generate tabular data.
    :param data: JSON string.
    :return: a dict.
    '''
    try:
      return json.loads(data)
    except:
      _logger.error('Error parsing data as JSON string. '+str(data), exc_info=True, stack_info=True)
      return {'error':str(data)}  # Return the record in a field named "error".
  
  def process(self, data):
    '''
    :param data:
    :return: returns (number of records written, number of records published)
    '''
    parsed=None
    try:
      _logger.debug('Received data. '+str(data)[:100]+('...' if len(str(data))>100 else ''))
      parsed=self._parse(data)
    except:
      _logger.error('Error processing data. '+str(data), exc_info=True, stack_info=True)
    
    numWritten=0
    numPublished=0
    if parsed is not None:
      # Output the data.
      if self._bucket is not None: numWritten+=self._writeToBucket(parsed)
      if self._publisher is not None: numPublished+=self._publish(parsed)
    return (numWritten, numPublished)

def callAPI(url,headers,parameters,projectId,topic,bucket,pathInBucket,debug):
  # Access API.
  _logger.info('Calling {url} with {params}.'.format(url=url, params=str(parameters)))
  
  data=None
  try:
    session=Session()
    session.headers.update(headers)
    response=session.get(url, params=parameters)
    data=json.loads(response.text)
  except:
    _logger.error('Error retrieving data.', exc_info=True, stack_info=True)
  
  if data is not None:
    processor=DataProcessor(projectId=projectId, topic=topic, bucket=bucket, path=pathInBucket, debug=debug)
    numWritten, numPublished=processor.process(data)
    _logger.info(
      'Wrote {numWritten:d} records to gs://{bucket}/{path}, published {numPublished:d} messages to {topic}.'.format(
        numWritten=numWritten,
        numPublished=numPublished,
        bucket=bucket,
        path=pathInBucket,
        topic=topic
      ))

def cloudFunctionMain(request):
  """Responds to any HTTP request.
  Args:
      request (flask.Request): HTTP request object.
  Returns:
      The response text or any set of values that can be turned into a
      Response object using
      `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  messageJSON=_getMessageJSON(request)
  
  debug=messageJSON.get('debug', None)
  if debug is not None: _logger.setLevel(debug)
  
  _logger.info('Trigger message received is '+json.dumps(messageJSON))
  
  pathInBucket=messageJSON.get('path', None)
  
  bucket=messageJSON.get('bucket', None)
  if bucket=='': bucket=None
  if bucket is not None:
    _logger.info('Using gs://{bucket}{pathInBucket} for storing records.'.format(bucket=bucket,
                                                                                 pathInBucket='' if pathInBucket is None else '/'+pathInBucket))
  
  projectId=messageJSON.get('projectId', '')
  if projectId=='': projectId=None
  
  topic=messageJSON.get('topic', '')
  if topic=='': topic=None
  if topic is not None:
    if projectId is None:
      _logger.error('Must include a project ID if you include a topic.')
      return 'Error attempting to access Pub/Sub topic with no project ID.'
    _logger.info('Will submit to {topic}.'.format(topic=topic))
  
  # Grab the parameters and headers from the message.
  parameters=messageJSON.get('parameters',None)
  headers=messageJSON.get('headers',None)
  callAPI(parameters,projectId,topic,bucket,pathInBucket,debug)
  
  return json.dumps(messageJSON)+' completed.'

if __name__=='__main__':
  '''
  Added statements that parse command-line arguments for testing. You can directly execute this Python code from the
  command-line and it will mimic what happens when a Cloud Function is triggered. Call this code as to see how all the
  options you can use when executing it:
      PYTHONPATH=~/classResources/python python ~/classResources/python/flight/stream/openSkyParser.py -h
  (Prepending the python command with "PYTHONPATH=..." adds the files within the ~/classResources/python directory to
   the path of available Python libraries.)
  '''
  defaultProjectId=os.environ.get('GOOGLE_CLOUD_PROJECT', 'no_project')
  
  parser=ArgumentParser(description='Pull a sample of flight streaming data from OpenSky and store in Google Cloud.')
  parser.add_argument('-log', action='store_true', help='Print out log statements.')
  parser.add_argument('-credentials',
                      help='Provide a file name of a local file which has credentials for Google Cloud.', default=None)
  
  parser.add_argument('-storage', action='store_true', help='Store as files in Google Cloud Storage.')
  parser.add_argument('-pubsub', action='store_true', help='Write to a Pub/Sub queue.')
  
  storageArgs=parser.add_argument_group('storage')
  storageArgs.add_argument('-bucket', help='The name of the bucket where data is to be stored.', default=None)
  storageArgs.add_argument('-path', help='The path within the bucket where data is to be stored.',
                           default='flights_streaming')
  
  pubsubArgs=parser.add_argument_group('pub/sub')
  pubsubArgs.add_argument('-projectId', help='The ID of the project that contains the Pub/Sub queue.',
                          default=defaultProjectId)
  pubsubArgs.add_argument('-topic', help='The Pub/Sub topic to write data to.', default=None)
  
  # parse the arguments
  args=parser.parse_args()
  
  credentials={}
  if args.credentials is not None:
    try:
      with open(args.credentials) as credentialsContent:
        credentials=json.loads('\n'.join(credentialsContent.readlines()))
    except:
      _logger.error('Cannot load local credentials from path '+args.credentials, exc_info=True, stack_info=True)
