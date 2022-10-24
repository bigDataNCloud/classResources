import unittest
from twitter.twitterParser import ExampleRequest,parseTweets

class TestTwitterParser(unittest.TestCase):
  _projectId='prof-big-data'
  _limit=10
  _topic='tweets-carsharing'
  _query=['zipcar','turo','getaround','gig car share','carshare']
  _delim=''
  
  def test_parseTweets(self):
    exampleRequest=ExampleRequest(self._projectId, self._query, limit=self._limit, topic=self._topic,
                                  userTopic=self._topic+'_user', bucket=self._projectId+'_data',
                                  pathInBuckets=self._topic,
                                  delim=self._delim,
                                  debug=10)
    parseTweets(exampleRequest)
    self.assertEqual(True, False)  # add assertion here

if __name__=='__main__':
  unittest.main()
