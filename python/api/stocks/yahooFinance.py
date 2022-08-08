import yfinance as yf
from argparse import ArgumentParser
import os
import json
import logging
from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

_allStocksFile='jp_files/Programs/stocks.csv'
_storageClient=None

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
  return _storageClient

def _store(bucket,path,data):
  '''
  An action that stores the data in the bucket at the given path.
  Args:
    bucket:
    path:
    data:
  Returns:
  '''
  try:
    return _getStorageClient(bucket).blob(path).upload_from_string(data.to_csv())
  except:
    _logger.error('Cannot write to '+path+' in '+bucket,exc_info=True,stack_info=True)

def _publish(projectId,topic,data):
  '''
  An action that writes the data to the given topic.
  Args:
    projectId:
    topic:
    data:
  Returns:
  '''
  try:
    pass
    # NOT YET IMPLEMENTED
  except:
    _logger.error('Cannot publish to '+topic,exc_info=True,stack_info=True)
    
def _parse(stock,period,interval,action):
  '''
  Query the API for the data on the given stock and act on it.
  Args:
    stock:
    period:
    interval:
    action: a function that takes the data returned by the API and acts on it.
  Returns:
  '''
  return action(yf.download(tickers=stock, period=period, interval=interval))

def parseAll(allStocksFile,bucket,path,period,interval):
  for symbol in _getStorageClient(bucket).blob(_allStocksFile).download_as_bytes().decode('utf-8').split('\n'):
    cleanedSymbol=symbol.strip()
    _logger.debug('Parsing '+cleanedSymbol)
    action=lambda data: _store(bucket,path+'/'+cleanedSymbol+'.csv',data)
    _parse(cleanedSymbol,period,interval,action)

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

def entry(request):
  defaultProjectId=os.environ.get('GOOGLE_CLOUD_PROJECT','no_project')
  _logger.debug('request is '+str(request)+' with args '+str(request.args))
  request_json=request.get_json()
  message={}
  if request_json is not None and 'message' in request_json: message=request_json['message']
  bucket=message.get('bucket',defaultProjectId+'_data')
  path=message.get('path','stocks')
  period=message.get('period','10y')
  interval=message.get('interval','1d')
  parseAll(_allStocksFile,bucket,path,period,interval)

if __name__ == '__main__':
  '''
  This segment of code will run when you execute the code from the command line (i.e., not when it is run from within a
  cloud function.) I use it to test the code before wrapping it in a cloud function.
  
  To run it from the command line, you will need to point python to the directory that has the code. You can set the
  PYTHONPATH at the beginning of the statement to execute, like so:
      PYTHONPATH=~/classResources/python python ~/classResources/python/api/stocks/yahooFinance.py -h
  '''
  defaultProjectId=os.environ.get('GOOGLE_CLOUD_PROJECT','no_project')
  parser=ArgumentParser()
  parser.add_argument('-bucket',default=defaultProjectId+'_data')
  parser.add_argument('-path',default='stocks')
  parser.add_argument('-period',default='10y')
  parser.add_argument('-interval',default='1d')
  args = parser.parse_args()
  parseAll(_allStocksFile,args.bucket,args.path,args.period,args.interval)