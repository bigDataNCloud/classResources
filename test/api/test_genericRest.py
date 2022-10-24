from api.genericRest import callAPI

projectId='prof-big-data'
topic=None
bucket='prof-big-data_data'
pathInBucket='test'
debug=10

bike
zipcar?
weather
openweathermap.org/api

url='https://investors-exchange-iex-trading.p.rapidapi.com/stock/GOOG/short-interest'
url='https://investors-exchange-iex-trading.p.rapidapi.com/stock/crm/time-series'
headers={
  "X-RapidAPI-Key":"c0f8357fa0mshb0bd516865b0ba6p197c65jsna52b2a082730",
  "X-RapidAPI-Host":"investors-exchange-iex-trading.p.rapidapi.com",
  'Accepts':'application/json'
}
parameters={'symbol':'GOOG'}

callAPI(url,headers,parameters,projectId,topic,bucket,pathInBucket,debug)