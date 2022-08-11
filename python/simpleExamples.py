from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient,SubscriberClient,DeleteSubscriptionRequest

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
    future.result(timeout=duration) # Wait and read messages for duration seconds.
    subscriber.delete_subscription(subscription=subscriptionPath) # Delete the subscription we just created.

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