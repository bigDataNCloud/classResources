from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient,SubscriberClient
from concurrent.futures._base import TimeoutError
from datetime import date,datetime
import json

def downloadFromStorage(bucketName,pathInBucket):
  storageClient=storage.Client()
  bucket=storageClient.bucket(bucketName)
  fileContents=bucket.blob(pathInBucket).download_as_bytes()
  storageClient.close()
  textContents=fileContents.decode('utf-8')
  return textContents

def uploadToStorage(bucketName,pathInBucket,data):
  storageClient=storage.Client()
  bucket=storageClient.bucket(bucketName)
  bucket.blob(pathInBucket).upload_from_string(data)
  storageClient.close()

def publish(projectId,topicName,message):
  pubsubClient=PublisherClient()
  topicPath='projects/'+projectId+'/topics/'+topicName
  futurePublish=pubsubClient.publish(topicPath,message.encode())
  futurePublish.result() # Will not actually publish the message until you call "result()".

subscriptionData=[]

def _readMessage(message):
  '''
  This method is called each time a new message is published.
  It will add the data of the message (after it decodes it from bytes to string) to the list subscriptionData.
  '''
  subscriptionData.append(message.data.decode('utf-8'))
  message.ack()

def subscribe(projectId,topicName,subscriptionName,duration=60):
  topicPath='projects/'+projectId+'/topics/'+topicName
  subscriptionPath='projects/'+projectId+'/subscriptions/'+subscriptionName
  
  with SubscriberClient() as subscriber:
    subscriber.create_subscription(name=subscriptionPath, topic=topicPath)
    future=subscriber.subscribe(subscriptionPath, _readMessage)
    try:
      future.result(timeout=duration) # Wait and read messages for duration seconds.
    except TimeoutError:
      print('Stopped waiting for messages since no new messages were published after '+str(duration)+'s.')
    subscriber.delete_subscription(subscription=subscriptionPath) # Delete the subscription we just created.

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
  return item # Return as a string if all the other attempts through exceptions.

def convertToJson(csvData,columns):
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
  return json.dumps(dict(filter(lambda column_value:type(column_value[1])!=str or len(column_value[1])>0,zip(columns, map(convertType,csvData.split(','))))))

def publishAsJson(projectId,topicName,csvData,columns):
  '''
  This does the same thing as the publish() method above but uses the convertToJson() method to translate the csv data
  into JSON format.
  Args:
    projectId:
    topicName:
    csvData: a string with data delimited by commas.
  '''
  pubsubClient=PublisherClient()
  topicPath='projects/'+projectId+'/topics/'+topicName
  # Convert the csv data to a JSON string.
  jsonString=convertToJson(csvData,columns)
  futurePublish=pubsubClient.publish(topicPath,jsonString.encode())
  futurePublish.result() # Will not actually publish the message until you call "result()".



# Example: Read text data from storage.
myBucket='prof-big-data_data'
myPath='data/flightsETL/2018-10.csv'
flightData=downloadFromStorage(myBucket,myPath)
print('Downloaded '+myPath+' from '+myBucket+': '+flightData[0:2048]) # Print the first 2048 characters of the data.

# Example: Write text data to storage.
myTestPath='testFile.csv'
uploadToStorage(myBucket,myTestPath,flightData[0:2048])

# Example: Write a message to Pub/Sub.
myTopic='myTestTopic'
projectId='prof-big-data'
publish(projectId,myTopic,'What a wonderful bird the frog are.')

# Example: Read (and consume) everything in a Pub/Sub topic. (WARNING: This will make all of the messages unavailable
# for any other subscribers.)
# Also, you will need to publish data to the topic after you create the subscription and before the timeout.
subscribe(projectId,myTopic,'myTestSubscription')

print('Retrieved '+str(len(subscriptionData))+' messages: ')
for message in subscriptionData:
  print(message)

csvData='2022-08-10,167.67999267578125,169.33999633789062,166.89999389648438,169.24000549316406,169.24000549316406,70076000'
columns=['date','open','high','low','close','adj_close','volume','symbol']

print('Original Data: '+csvData)
print('JSON Data: '+convertToJson(csvData,columns))

publishAsJson(projectId,myTopic,csvData,columns)

vaccineData='2021-01-15,Alabama,100567.0,444650.0,86956.0,0.28,2.05,13488.0,1.77,9.07,8267.0,7478.0,1525.0,0.226,333,444'
vaccineColumns=['date', 'location', 'total_vaccinations', 'total_distributed', 'people_vaccinated', 'people_fully_vaccinated_per_hundred', 'total_vaccinations_per_hundred', 'people_fully_vaccinated', 'people_vaccinated_per_hundred', 'distributed_per_hundred', 'daily_vaccinations_raw', 'daily_vaccinations', 'daily_vaccinations_per_million', 'share_doses_used', 'total_boosters', 'total_boosters_per_hundred']
convertToJson(vaccineData,vaccineColumns)