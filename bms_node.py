import time
import getopt
import sys
import logging

from collections import OrderedDict
from pyndn import Name, Data, Interest
from pyndn.threadsafe_face import ThreadsafeFace

from pyndn.security import KeyChain
from pyndn.util.common import Common

try:
	import asyncio
except ImportError:
	import trollius as asyncio

from config_split import BoostInfoParser

DEFAULT_LIFETIME = 6000

class Aggregation(object):
	def __init__(self):
		pass

	def getAggregation(self, aggregationType, *dataList):
		if len(dataList) == 0:
			assert failedse, 'DataList is None'
		if aggregationType == 'avg':
			return self.getAvg(dataList)
		elif aggregation == 'min':
			return self.getMin(dataList)
		elif aggregation == 'max':
			return self.getMax(dataList)
		else:
			assert False, 'Not implemented'

	def getAvg(self, *dataList):
		return reduce(lambda x, y: x, x + y, dataList) / float(len(dataList))

	def getMin(self, *dataList):
		return min(dataList)

	def getMax(self, *dataList):
		return max(dataList)


class BmsNode(object):
	def __init__(self):
		self.boost = None
		self.lastRefreshTime = int(time.time())
		self._keyChain = None
		self._certificateName = None

		self._tempName = None
		self._tempData = None
		self._dataQueue = []
		self.aggregation = Aggregation()

	def setConfiguration(self, fileName):
		self.boost = BoostInfoParser()
		self.boost.read(fileName)

	def startPublishing(self):
		self.prepareLogging()
		self._keyChain = KeyChain()

		self._loop = asyncio.get_event_loop()
		self._face = ThreadsafeFace(self._loop)

		self._face.setCommandSigningInfo(self._keyChain, self._keyChain.getDefaultCertificateName())

		self.lastRefreshTime = int(time.time())
		dataNode = self.boost.getDataNode()
		childrenNode = self.boost.getChildrenNode()

		self._face.registerPrefix(Name(self.boost.getNodePrefix()), self.onInterest, self.onRegisterFailed)

		# For each type of data, we refresh each type of aggregation according to the interval in the configuration
		for i in range(len(dataNode.subtrees)):
			dataType = dataNode.subtrees.keys()[i]
			aggregationParams = self.boost.getProducingParamsForAggregationType(dataNode.subtrees.items()[i][1])

			for aggregationType in aggregationParams:
				if aggregationType != "raw":
					children = OrderedDict()

					for j in range(len(childrenNode.subtrees)):
						if dataType in childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees:
							if aggregationType in childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees[dataType].subtrees:
								children[childrenNode.subtrees.items()[j][0]] = self.boost.getProducingParamsForAggregationType(childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees[dataType].subtrees[aggregationType])
								#print("add child: " + childrenNode.subtrees.items()[j][0] + "-" + dataType + "-" + aggregationType)
							
					self.startPublishingAggregation(aggregationParams[aggregationType], children, dataType, aggregationType)
				else:
					self.startPublishingRaw(aggregationParams[aggregationType])
				#self._doRefresh(time, refreshInterval.items()[j][1], dataType, refreshInterval.items()[j][0])
		return

	def startPublishingRaw(self, params):
		return

	def startPublishingAggregation(self, params, childrenList, dataType, aggregationType):
		if __debug__:
			print('Start publishing for ' + dataType + '-' + aggregationType)
		publishingPrefix = Name(self.boost.getNodePrefix()).append('name').append(dataType).append('aggregation').append(aggregationType)

		for child in childrenList.keys():
			name = Name(self.boost.getNodePrefix()).append(child).append('name').append(dataType).append('aggregation').append(aggregationType)
			interest = Interest(name)
			if ('start_time' in childrenList[child]):
				interest.getName().append(params['start_time']).append(params['producer_interval'])
			else:
				interest.setChildSelector(1)
			interest.setInterestLifetimeMilliseconds(DEFAULT_LIFETIME)
			if __debug__:
				print('  Issue interest: ' + interest.getName().toUri())
			self._face.expressInterest(interest, self.onData, self.onTimeout)

		return

	def _doRefresh(self, time, producerInterval, dataType, aggregationType):
		now = self.lastRefreshTime
		if now >= time + producerInterval:
			if aggregation == "inst":
				runConsumer(None, dataType, aggregationType, now)
				self._dataQueue.append((self._tempName, str(self._tempData)))
				self._tempData = None
			elif aggregation == "batch":
				runConsumer(None, dataType, aggregationType, time, now)
				self._dataQueue.append((self._tempName, str(self._tempData)))
				self._tempData = None
			elif aggregation == "raw":
				# not finish
				pass
			else: 
				#aggregation in ["avg", "min", "max"]:
				dataList = []
				for i in range(len(self.boost.subtrees['children'].subtrees)):
					string = self.boost.subtrees['children'].subtrees.items()[i][0]
					self.runConsumer(string, dataType, aggregationType, time, now)
					if not self._tempData == None:
						dataList.append(self._tempData)
						self._tempData = None
				self._tempData = self.aggregation._doRefresh(aggregationType, dataList)
				self._dataQueue.append((self._tempName, str(self._tempData)))
				self._tempData = None
		face = self.registerPrefix()
		self.runProducer(face)

	def setName(self, prefix, dataType, aggregationoType, startTime, endTime = None):
		if endTime == None:
			string = self.boost.getName(prefix) + '/data/' + dataType + '/' + aggregationType + '/' + startTime
		else:
			string = self.boost.getName(prefix) + '/data/' + dataType + '/' + aggregationType + '/' + startTime + '/' + endTime		
		self._tempName = Name(string)
		return name

	def runProducer(self, face):
		while self._responseCount < 1:
			face.processEvents()
			time.sleep(0.01)

		face.shutdown()

	def runConsumer(self, prefix, dataType, aggregationType, startTime, endTime = None):
		face = Face()
		self._callbackCount = 0
		name = Name(self.setName(prefix, dataType, aggregationType, startTime, endTime))
		face.expressInterest(name, self.onData, self.onTimeout)

		while self._callbackCount < 1:
			face.processEvents()
			time.sleep(0.01)

		face.shutdown()

	def onInterest(self, prefix, interest, face, interestFilterId, filter):
		name = interest.getName()

		for i in range(len(self._dataQueue)):
			if name == self._dataQueue[i][0]:
				content = self._dataQueue[i][1]
				# self._dataQueue.pop(i)

		data = Data(name)
		data.setContent(content)
		self._keyChain.sign(data, self._certificateName)
		face.putData(data)

	def onRegisterFailed(self, prefix):
		raise RuntimeError("Register failed for prefix", prefix.toUri())

	def onData(self, interest, data):
		string = data.getContent().toRawStr()
		self._tempData = int(string)

	def onTimeout(self, interest):
		if __debug__:
			print("interest timeout: " + interest.getName().toUri())
		self._face.expressInterest(interest, self.onData, self.onTimeout)
		return

	def stop(self):
		self._loop.stop()
		if __debug__:
			print("Stopped")
		return

##
# Logging
##
	def prepareLogging(self):
		self.log = logging.getLogger(str(self.__class__))
		self.log.setLevel(logging.DEBUG)
		logFormat = "%(asctime)-15s %(name)-20s %(funcName)-20s (%(levelname)-8s):\n\t%(message)s"
		self._console = logging.StreamHandler()
		self._console.setFormatter(logging.Formatter(logFormat))
		self._console.setLevel(logging.INFO)
		# without this, a lot of ThreadsafeFace errors get swallowed up
		logging.getLogger("trollius").addHandler(self._console)
		self.log.addHandler(self._console)

	def setLogLevel(self, level):
		"""
		Set the log level that will be output to standard error
		:param level: A log level constant defined in the logging module (e.g. logging.INFO) 
		"""
		self._console.setLevel(level)

	def getLogger(self):
		"""
		:return: The logger associated with this node
		:rtype: logging.Logger
		"""
		return self.log

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "", ["conf="])
	except getopt.GetoptError as err:
		print err
		usage()
		sys.exit(2)
	for o, a in opts:
		if o == "--conf":
			bNode = BmsNode()
			bNode.setConfiguration(a)
			bNode.startPublishing()


			try:
				bNode._loop.run_forever()
			except Exception as e:
				print(e)
			finally:
				bNode.stop()
		else:
			assert False, "unhandled option"

main()

	


