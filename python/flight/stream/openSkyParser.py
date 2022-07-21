# The following code retrieves flight data using OpenSky's API and produces rows as JSON objects that can be
# loaded directly into a Big Query table in Google Cloud.
# This file is set up to work with Cloud Function and can be run with an empty message.
# Alternatively you can pass in parameters to control a few aspects:
#   debug: will spit out occassional debug statements if this flag is present.
#   bucket: Google Cloud Storage bucket for storing the data (defaults to your project ID + "_data".)
#   path: path within the bucket to process the data in (defaults to "flights_streaming".)
#   separateLines: will create a separate file/message for each record instead of a file/message
#                  for all records returned from the API call if this flag is present.
#
# You can test out this code from the command-line:
#   (Make sure to set your PYTHONPATH to include the code, such as the following for a LINUX system, such as from Cloud Shell:
#      export PYTHONPATH=~/classResources/python
#   )
#   python ~/classResources/python/flight/stream/main_flight-streaming.py -log -storage
# Use the following tests from the Cloud Function UI:
#   Write to Google Storage and publish to a Pub/Sub queue simultaneously:
#      {"storage":true,"pubsub":true,"limit":10}
#   If you only want to publish messages to Pub/Sub and want to specify a topic and project:
#      {"pubsub":true,"topic":"my-topic","projectId":"my-project"}
#   If you only want to write to GCS and want to specify a bucket and path.
#      {"storage":true,"bucket":"my-bucket","path":"my-path"}

# OpenSky API is provided free of charge for non-commercial use.
# See: https://opensky-network.org/
# For the Python API, see: https://opensky-network.org/apidoc/python.html
import os
from argparse import ArgumentParser
import json
import logging

from google.cloud import storage
import datetime

from google.cloud.pubsub_v1 import PublisherClient
from google.oauth2 import service_account

from flight.stream.opensky_api import OpenSkyApi

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

numTries=5 # Number of times to try to get data from OpenSky.
defaultLimit=30

class RequestTemplate(object):
  '''
  Mimics a request used to trigger a Cloud Function. Instances of this class are filled with properties and passed to the
  parser when running from the command-line.
  '''
  args = None
  def __init__(self,
               query='',limit=None,debug=False,separateLines=True,
               projectId='',topic='',
               bucket='',path='',storage=False,pubsub=False):
    if query is not None:
      if type(query) == str:
        if not query.startswith('"'): query = '"' + query + '"'
      else:
        query = json.dumps(query)
    else:
      query=''
    message = {'storage':storage,'pubsub':pubsub,
               'query': query, 'limit': limit if limit is not None else '',
               'projectId': projectId,
               'topic': topic if topic is not None else '',
               'bucket': bucket if bucket is not None else '',
               'path': path if path is not None else ''}
    if separateLines: message['separateLines']=True
    self.args = {'message': json.dumps(message)}
  
  def get_json(self):
    return json.loads(self.args['message'])

def _getMessageJSON(request):
  '''
  A request that triggers a Cloud Function can be formatted in a variety of ways. (Some of these may now be legacy.)
  This method uses some trial and error to search for the message within the request.
  :request: passed into a Cloud Function when it is triggered.
  :return: returns an object parsed from the message if the message can be identified, otherwise None.
  '''
  request_json = request.get_json()
  message = None

  if request.args is not None:
    _logger.debug('request is ' + str(request) + ' with args ' + str(request.args))
    if 'message' in request.args: message = request.args.get('message')

    if any(map(lambda param:param in request.args,['bucket','path','topic','projectId'])):
      # request.args holds the fields we are expecting to exist in the message.
      message = request.args
  if message is None and request_json is not None:
    # If message remains unset (None) then assuming that the request_json holds the contents of the message we are looking for.
    _logger.debug('request_json is ' + str(request_json))
    if 'message' in request_json:
      message = request_json['message']
    else:
      message = request_json
  
  if message is None:
    _logger.warning('message is empty. request=' + str(request) + ' request_json=' + str(request_json))
    message = '{}'
  
  if type(message) == str:
    # If message type is str and not dict, attempt to parse as a JSON string.
    try:
      messageJSON = json.loads(message)
    except:
      try:
        _logger.error('ERROR Cannot parse provided message ' + str(message),exc_info=True,stack_info=True)
      except:
        pass
      messageJSON = {}
  else:
    # Else, assuming messageJSON was decoded from a JSON object.
    messageJSON = message
  return messageJSON

class Storage(object):
  '''
  The Storage class handles writing parsed output to Cloud Storage.
  '''
  _increment = 0
  
  def _createFileName(self):
    '''
    :return: returns a unique file name to store results in.
    '''
    filename = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_' + str(self._increment)
    self._increment += 1
    return filename
  
  def __init__(self, bucket, folder=None, separateLines=False, project=None, credentials=None):
    self._bucket = bucket
    if credentials is not None:
      gcClient=storage.Client(
        project=credentials['project_id'],
        credentials=service_account.Credentials.from_service_account_info(credentials)
      )
    elif project is not None:
      gcClient=storage.Client(project=project)
    else:
      gcClient=storage.Client()
    self._client = gcClient.bucket(self._bucket)
    self._path = ('flightData' if folder is None else folder)
    self._separateLines = separateLines
  
  def process(self, data):
    '''
    Will write data as a series of JSON objects, one per line. NOTE that this is not a JSON list of JSON objects. Big Query will ingest the series of JSON objects on separate lines.
    :param data (list): a list of dicts representing records.
    '''
    rows = map(lambda row: json.dumps(row), data)
    if self._separateLines:
      _logger.debug(json.dumps({'log': 'Storing as separate files within {path}.'.format(path=self._path)}))
      for row in rows:
        fullpath=self._path + '/' + self._createFileName()
        try:
          self._client.blob(fullpath).upload_from_string(row)
        except Exception as ex:
          _logger.error('Error writing to {path}'.format(path=fullpath),exc_info=True,stack_info=True)
    else:
      fullpath=self._path + '/' + self._createFileName()
      _logger.debug('Storing in file {path}.'.format(path=self._path + '/' + self._createFileName()))
      try:
        self._client.blob(fullpath).upload_from_string('\n'.join(rows))
      except Exception as ex:
        _logger.error('Error writing to {path}'.format(path=fullpath),exc_info=True,stack_info=True)

class Publish(object):
  '''
  Publishes a message in a Pub/Sub queue when the process method is called.
  '''
  _increment = 0

  def _createKey(self):
    '''
    :return: returns a unique key for each entry.
    '''
    key = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_' + str(self._increment)
    self._increment += 1
    return key
  
  def __init__(self, projectId, topic, separateLines=False, credentials=None):
    self._topicPath='projects/{project}/topics/{topic}'.format(project=projectId,topic=topic)
    if credentials is not None:
      self._publisher=PublisherClient(
        credentials=service_account.Credentials.from_service_account_info(credentials)
      )
    else:
      self._publisher=PublisherClient()
    self._separateLines = separateLines
  
  def process(self, data):
    '''
    Will write data as a series of JSON objects, one per line. NOTE that this is not a JSON list of JSON objects. Big Query will ingest the series of JSON objects on separate lines.
    :param data (list): a list of dicts representing records.
    '''
    rows = map(lambda row: json.dumps(row,sort_keys=True), data)
    if self._separateLines:
      futures=[]
      for row in rows:
        key=self._createKey()
        try:
          # Very verbose logging: _logger.debug('Publishing: '+row)
          futures.append(self._publisher.publish(self._topicPath, data=row.encode('utf-8'), query=key))
        except:
          _logger.error('Error publishing {key} to {topic}'.format(key=key,topic=self._topicPath),exc_info=True,stack_info=True)
      numMessagesPublished=0
      for index,futurePublish in enumerate(futures):
        try:
          # Very verbose logging: _logger.debug('Published message ID: '+str(futurePublish.result()))
          futurePublish.result()
          numMessagesPublished+=1
        except:
          _logger.error('Error while publishing message #'+str(index),exc_info=True,stack_info=True)
      _logger.debug('Published '+str(numMessagesPublished)+' messages.')
    else:
      key=self._createKey()
      try:
        self._publisher.publish(self._topicPath, data=('\n'.rows).encode('utf-8'), query=self._createKey())
      except:
        _logger.error('Error publishing {key} to {topic}'.format(key=key, topic=self._topicPath),exc_info=True,stack_info=True)

def _convertTimestamp(timestamp):
  '''
  Convert from system timestamp into a format for time that Big Query can understand.
  :timestamp (int): time in seconds.
  :return (datetime): a string representation of a date and time or else None if the translation failed.
  '''
  if timestamp is not None:
    try:
      return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except:
      pass
  return None

def _convert(data,dataType):
  if data is not None:
    if dataType==str: return data.strip()
    elif dataType==int: return int(data)
    elif dataType==float: return float(data)
    elif dataType==bool: return bool(data)
    return data
  return None

def _convertRow(flightState, queryTime):
  '''
  :param flightState:
  :param queryTime:
  :return (dict): returns a dict of the given row that can be stored as a JSON dump.
  '''
  row = {
    'icao24': _convert(flightState.icao24,str),
    # icao24 - ICAO24 address of the transmitter in hex string representation.
    'callsign': _convert(flightState.callsign,str),
    # callsign of the vehicle. Can be None if no callsign has been received.
    'origin': _convert(flightState.origin_country,str),  # inferred through the ICAO24 address
    'time':_convert( flightState.time_position,int),
    # seconds since epoch of last position report. Can be None if there was no position report received by OpenSky within 15s before.
    'contact':_convert( flightState.last_contact,int),
    # seconds since epoch of last received message from this transponder
    'longitude':_convert( flightState.longitude,float),
    # in ellipsoidal coordinates (WGS-84) and degrees. Can be None
    'latitude':_convert( flightState.latitude,float),
    # in ellipsoidal coordinates (WGS-84) and degrees. Can be None
    'altitude':_convert( flightState.geo_altitude,float),  # geometric altitude in meters. Can be None
    'on_ground':_convert( flightState.on_ground,bool),
    # true if aircraft is on ground (sends ADS-B surface position reports).
    'velocity':_convert( flightState.velocity,float),
    # velocity - over ground in m/s. Can be None if information not present
    'heading':_convert( flightState.heading,float),
    # in decimal degrees (0 is north). Can be None if information not present.
    'vertical_rate':_convert( flightState.vertical_rate,float),
    # vertical_rate - in m/s, incline is positive, decline negative. Can be None if information not present.
    'sensors':_convert( flightState.sensors,str),
    # sensors - serial numbers of sensors which received messages from the vehicle within the validity period of this state vector. Can be None if no filtering for sensor has been requested.
    'baro_altitude':_convert( flightState.baro_altitude,float),
    # baro_altitude - barometric altitude in meters. Can be None
    'squawk':_convert( flightState.squawk,int),  # squawk - transponder code aka Squawk. Can be None
    'spi':_convert( flightState.spi,bool),  # spi - special purpose indicator
    'position_source':_convert( flightState.position_source,int)
    # position_source - origin of this state’s position: 0 = ADS-B, 1 = ASTERIX, 2 = MLAT, 3 = FLARM
  }
  # Following are additional fields added by this code to create timestamps that can be used with BigQuery as date fields.
  time_bq = _convertTimestamp(flightState.time_position)
  if time_bq is not None: row['time_bq'] = time_bq if time_bq is not None else ""
  contact_bq = _convertTimestamp(flightState.last_contact)
  if contact_bq is not None: row['contact_bq'] = contact_bq if contact_bq is not None else ""
  query_time_bq = _convertTimestamp(queryTime)
  if query_time_bq is not None: row['query_time_bq'] = query_time_bq if query_time_bq is not None else ""
  return row

def _getLatestFlightData():
  api = OpenSkyApi()
  for trial in range(numTries):
    try:
      _logger.debug('Requesting latest flights from OpenSky.')
      flightStates = api.get_states()
      if flightStates is not None: return flightStates
    except:
      _logger.error('Failed in call to OpenSky.',exc_info=True)
  return None

def _scavengeRows(separateLines=False,
                  bucket=None,path=None,
                  projectId=None,topic=None,
                  debug=None,limit=None,
                  credentials=None):
  '''
  :param separateLines: output each flight record as a separate item if True.
  :param bucket: output to a bucket in GCS if not null.
  :param path: the path of a directory within the bucket to write output to.
  :param projectId: the project ID to use (required if not run within Google resources or if publishing to a Pub/Sub queue).
  :param topic: the Pub/Sub topic to use if publishing to a queue.
  :param debug: set to 10 to see debug statements.
  :param limit: a limit on the number of rows to write/publish.
  :param credentials: expecting a dict with keys for type,project_id,private_key_id,private_key,client_email,client_id,auth_url,token_url,auth_provider_x509_cert_url,client_x509_cert_url;
         this is optional; no need to pass in credentials when run from within Google's infrastructure.
  '''
  queryTime = datetime.datetime.now().timestamp()
  if debug is not None:
    _logger.debug(json.dumps({'log': 'Scavenging rows at {queryTime}.'.format(queryTime=str(queryTime))}))
  flightStates=_getLatestFlightData()
  numProcessed=0
  if flightStates is not None:
    records = []
    for flightDict in map(lambda flightState: _convertRow(flightState, queryTime), flightStates.states):
      trimmedRecord = dict(filter(lambda item: item[1] is not None, flightDict.items()))
      if len(trimmedRecord) > 0:
        try:
          # If the record has at least one non-empty field, process it.
          records.append(trimmedRecord)
        except Exception as ex:
          _logger.error('ERROR cannot process record.',exc_info=True,stack_info=True)
      if limit is not None and len(records)>limit: break
    
    if len(records) > 0:
      if debug is not None: _logger.debug(json.dumps({'log': 'Found {num:d} records to process.'.format(num=len(records))}))
      # Found records to process and/or publish.
      if bucket is not None:
        storage = Storage(bucket, folder=path, separateLines=separateLines,project=projectId,credentials=credentials)
        storage.process(records)
        numProcessed+=len(records)
        if debug is not None: _logger.debug(json.dumps({
          'log': 'Stored {num:d} records in folder {path} of bucket {bucket}'.format(
            num=len(records), path=path, bucket=bucket)}))
      
      if topic is not None and projectId is not None:
        publisher=Publish(projectId,topic,separateLines=separateLines,credentials=credentials)
        publisher.process(records)
        numProcessed+=len(records)
        if debug is not None: _logger.debug(json.dumps({
          'log': 'Published {num:d} records to topic {topic}'.format(
            num=len(records), topic=topic)}))
  else:
    if debug is not None: _logger.debug(json.dumps({'log': 'No flight records were found.'}))
  return numProcessed

def parse(request,credentials=None):
  """Responds to any HTTP request.
  :request (flask.Request): HTTP request object, the request passed into a Cloud Function when triggered.
  :return: returns the response text or any set of values that can be turned into a Response object using
           `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  messageJSON = _getMessageJSON(request)
  
  debug = messageJSON.get('debug', 10)
  if debug == 0:
    debug = None
  else:
    _logger.setLevel(debug)
  store=messageJSON.get('storage',False)
  publish=messageJSON.get('pubsub',False)

  if publish:
    projectId=messageJSON.get('projectId','')
    projectId=projectId.strip()
    if len(projectId)==0: projectId=None
  
    topic=messageJSON.get('topic', '')
    topic=topic.strip()
    if topic=='': topic = None

  if store:
    bucket=messageJSON.get('bucket', '')
    bucket=bucket.strip()
    if bucket=='': bucket=None
    
    path=messageJSON.get('path', '')
    path=path.strip()
    if path=='': path=None
  
  separateLines = 'separateLines' in messageJSON
  limit = messageJSON.get('limit',None)
  if type(limit)==str:
    try:
      limit=int(limit)
    except:
      limit=None
  if limit is None: limit=defaultLimit
  
  _logger.info(json.dumps({'log': 'Parsed message is ' + json.dumps(messageJSON)}))
  if publish:
    _logger.info(json.dumps({'log':'Will publish to projectID:{project} topic:{topic}'.format(project=projectId,topic=topic)}))
  if store:
    _logger.info(json.dumps({'log':'Will store in GCS at bucket:{bucket} path:{path}'.format(bucket=bucket,path=path)}))
  numProcessed=_scavengeRows(separateLines=separateLines,
                bucket=bucket,path=path,
                projectId=projectId,topic=topic,
                debug=debug,limit=limit,
                credentials=credentials)
  return json.dumps(messageJSON)+' handled '+str(numProcessed)+' items.'

if __name__ == '__main__':
  '''
  Added statements that parse command-line arguments for testing. You can directly execute this Python code from the
  command-line and it will mimic what happens when a Cloud Function is triggered. Call this code as to see how all the
  options you can use when executing it:
      PYTHONPATH=~/classResources/python python ~/classResources/python/flight/stream/openSkyParser.py -h
  (Prepending the python command with "PYTHONPATH=..." adds the files within the ~/classResources/python directory to
   the path of available Python libraries.)
  '''
  defaultProjectId=os.environ.get('GOOGLE_CLOUD_PROJECT','no_project')
  
  parser = ArgumentParser(description='Pull a sample of flight streaming data from OpenSky and store in Google Cloud.')
  parser.add_argument('-separateLines',action='store_true',help='Store each flight record as a separate file or post as a separate pub/sub entry, otherwise will process and/or post all records received from one query as a single file/entry.')
  parser.add_argument('-query',default=None)
  parser.add_argument('-limit',help='The maximum number of entries to pull from OpenSky.',default=defaultLimit,type=int)
  parser.add_argument('-log',action='store_true',help='Print out log statements.')
  parser.add_argument('-credentials',help='Provide a file name of a local file which has credentials for Google Cloud.',default=None)

  parser.add_argument('-storage',action='store_true',help='Store as files in Google Cloud Storage.')
  parser.add_argument('-pubsub',action='store_true',help='Write to a Pub/Sub queue.')

  storageArgs=parser.add_argument_group('storage')
  storageArgs.add_argument('-bucket',help='The name of the bucket where data is to be stored.',default=None)
  storageArgs.add_argument('-path',help='The path within the bucket where data is to be stored.',default='flights_streaming')

  pubsubArgs=parser.add_argument_group('pub/sub')
  pubsubArgs.add_argument('-projectId',help='The ID of the project that contains the Pub/Sub queue.',default=defaultProjectId)
  pubsubArgs.add_argument('-topic', help='The Pub/Sub topic to write data to.',default=None)
  
  # parse the arguments
  args = parser.parse_args()
  
  credentials=None
  if args.credentials is not None:
    try:
      with open(args.credentials) as credentialsContent:
        credentials=json.loads('\n'.join(credentialsContent.readlines()))
    except:
      _logger.error('Cannot load local credentials from path '+args.credentials,exc_info=True,stack_info=True)
  
  requestArgs={}
  requestArgs['query']=args.query
  requestArgs['limit']=args.limit
  projectId=defaultProjectId if args.projectId is None else args.projectId
  if args.pubsub:
    requestArgs['pubsub']=args.pubsub
    requestArgs['projectId']=args.projectId
    requestArgs['topic']=args.topic
  if args.storage:
    requestArgs['storage']=args.storage
    requestArgs['path']=args.path
    requestArgs['bucket']=args.bucket if args.bucket is not None else projectId+'_data'
  exampleRequest = RequestTemplate(**requestArgs)
  
  parse(exampleRequest,credentials=credentials)
