# The following code retrieves stock open/close data using a Yahoo Finance library and produces rows as JSON objects that can be
# loaded directly into a Big Query table in Google Cloud.
# This file is set up to work with Cloud Function. NOTE: The cloud function needs enough memory to download the
# stock data. I needed at least an instance with 256MB of memory to process a period of 7d.
#
# You can configure how the code is run using the message that is passed in when the cloud function is triggered:
#   debug: will spit out occassional debug statements. You can turn off debugging by setting it to 0.
#   projectId: the ID of your project.
#   bucket: Google Cloud Storage bucket for storing the data; defaults to projectId + "_data".
#   path: path within the bucket to process the data in (defaults to "stocks".)
#   period: defaults to 10y.
#   interval: defaults to 1 day ("1d").
#   addTimestamp: if "true" then place all the files within a folder named by a timestamp, otherwise will overwrite any file with the same name in the path you give.
#
# You can test out this code from the command-line:
#   Make sure to set your PYTHONPATH to include the code, such as the following for a LINUX system, such as from Cloud Shell:
#      PYTHONPATH=~/classResources/python python ~/classResources/python/api/stocks/yahooFinance.py -projectId prof-big-data -bucket prof-big-data_data -path week-of-stocks -interval 7d -period 1d
# Use the following tests from the Cloud Function UI:
#   Write to Google Storage and publish to a Pub/Sub queue simultaneously:
#   {
#        "debug":10,
#        "bucket":"prof-big-data_data",
#        "path":"stocks",
#        "projectId":"prof-big-data",
#        "interval":"1d",
#        "period":"7d",
#        "addTimestamp":"true"
#   }

import yfinance as yf
from argparse import ArgumentParser
import functions_framework
import os
import json
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient

logging.basicConfig(format='%(asctime)s.%(msecs)03dZ,%(pathname)s:%(lineno)d,%(levelname)s,%(module)s,%(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
_logger = logging.getLogger(__name__)

_allStocksFile='allStocks.csv'
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
  if not _storageClient.exists(): raise Exception('Cannot access bucket '+bucket)
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
  numStocks=0
  stocksFileContents=_getStorageClient(bucket).blob(_allStocksFile)
  if stocksFileContents.exists():
    symbols=stocksFileContents.download_as_bytes().decode('utf-8').split('\n')
  else:
    _logger.error('Cannot read stocks from '+allStocksFile+' in bucket '+bucket)
    symbols=['GOOGL','GLD','NFLX']
  for symbol in symbols:
    try:
      cleanedSymbol=symbol.strip()
      _logger.debug('Parsing '+cleanedSymbol)
      action=lambda data: _store(bucket,'{path}/symbol={symbol}/{symbol}.csv'.format(path=path,symbol=cleanedSymbol),data)
      _parse(cleanedSymbol,period,interval,action)
      numStocks+=1
    except:
      _logger.error('Cannot parse stocks for symbol '+symbol)
  return numStocks

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

  projectId=message.get('projectId',os.environ.get('GOOGLE_CLOUD_PROJECT','no_project'))
  bucket=message.get('bucket',projectId+'_data')
  path=message.get('path','stocks')
  period=message.get('period','10y')
  interval=message.get('interval','1d')
  addTimestamp=message.get('addTimestamp',None)
  if addTimestamp=='true':
    # Append a timestamp to the path so that we don't overwrite an existing set of files.
    path+='/timestamp='+datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
  _logger.info('Will parse all stocks in '+_allStocksFile+' for the period of '+period+' at the interval of '+interval+
               ', storing in path '+path+' of bucket '+bucket+', projectId='+str(projectId))
  numParsed=parseAll(_allStocksFile,bucket,path,period,interval)
  return 'Completed parsing '+str(numParsed)+' stocks.'

if __name__ == '__main__':
  '''
  This segment of code will run when you execute the code from the command line (i.e., not when it is run from within a
  cloud function.) I use it to test the code before wrapping it in a cloud function.
  
  To run it from the command line, you will need to point python to the directory that has the code. You can set the
  PYTHONPATH at the beginning of the statement to execute, like so:
      PYTHONPATH=~/classResources/python python ~/classResources/python/api/stocks/yahooFinance.py -h
  '''
  parser=ArgumentParser()
  parser.add_argument('-bucket',default=None)
  parser.add_argument('-path',default='stocks')
  parser.add_argument('-period',default='10y')
  parser.add_argument('-interval',default='1d')
  parser.add_argument('-projectId',default=None)
  args = parser.parse_args()
  projectId=os.environ.get('GOOGLE_CLOUD_PROJECT','no_project') if args.projectId is None else args.projectId
  bucket=projectId+'_data' if args.bucket is None else args.bucket
  parseAll(_allStocksFile,args.bucket,args.path,args.period,args.interval)