#
# A simple, configurable Throttler for requests that can be executed with .execute()
#


import time
import apiclient.errors


class SimpleThrottler():
    def __init__(self,throttleDelay=0.125,retryCount=3,retryBackoff=3):
        """Minimum time (in seconds) to wait between requests"""
        self.throttleDelay = throttleDelay
        self.retryCount=retryCount
        self.retryBackoff=retryBackoff
        self.lastRequestTime = None

    def execute(self, request):
        tries=0
        while True:
            currentTime = time.time()
            if self.lastRequestTime and (currentTime - self.lastRequestTime) < self.throttleDelay:
                sleepTime = self.lastRequestTime + self.throttleDelay - currentTime
                print "SimpleThrottler: Not enough time between requests, sleeping ", sleepTime, " seconds..."
                time.sleep(sleepTime)
            try:
                self.lastRequestTime = time.time()
                ret = request.execute()
                self.lastRequestTime = time.time()
                return ret
            except apiclient.errors.HttpError as e:
                print "Got HttpError while executing request: ", e
                tries += 1
                if tries > self.retryCount:
                    print "Retry count exceeded after ", tries, " retries, giving up..."
                    raise
                retrySleepTime = tries * self.retryBackoff
                print "sleeping ", retrySleepTime, " seconds before retrying..."
                time.sleep(retrySleepTime)
