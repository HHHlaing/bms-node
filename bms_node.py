import time
import getopt
import sys
import logging
import random

from collections import OrderedDict
from pyndn import Name, Data, Interest
from pyndn.threadsafe_face import ThreadsafeFace

from pyndn.security import KeyChain
from pyndn.util.common import Common
from pyndn.util import MemoryContentCache

try:
	import asyncio
except ImportError:
	import trollius as asyncio

from config_split import BoostInfoParser

DEFAULT_INTEREST_LIFETIME = 6000
DEFAULT_DATA_LIFETIME = 200000

class Aggregation(object):
	def __init__(self):
		pass

	def getAggregation(self, aggregationType, dataList):
		if len(dataList) == 0:
			print('DataList is None')
			return None
		if aggregationType == 'avg':
			return self.getAvg(dataList)
		elif aggregationType == 'min':
			return self.getMin(dataList)
		elif aggregationType == 'max':
			return self.getMax(dataList)
		else:
			assert False, 'Not implemented'

	def getAvg(self, dataList):
		if len(dataList) > 0:
			return float(sum(dataList))/len(dataList)
		else:
			return float('nan')

	def getMin(self, dataList):
		return min(dataList)

	def getMax(self, dataList):
		return max(dataList)

class DataQueue(object):
	def __init__(self, publishingParams):
		self._dataDict = dict()
		self._publishingParams = publishingParams
		return

class BmsNode(object):
	def __init__(self):
		self.boost = None
		self.lastRefreshTime = int(time.time())
		self._keyChain = None
		self._certificateName = None

		self._tempName = None
		self._tempData = None

		self._dataQueue = dict()
		self._memoryContentCache = None

		self._aggregation = Aggregation()

	def setConfiguration(self, fileName):
		self.boost = BoostInfoParser()
		self.boost.read(fileName)

	def onDataNotFound(self, prefix, interest, face, interestFilterId, filter):
		print('Data not found for ' + interest.getName().toUri())
		return

	def startPublishing(self):
		self.prepareLogging()
		self._keyChain = KeyChain()

		self._loop = asyncio.get_event_loop()
		self._face = ThreadsafeFace(self._loop)

		self._face.setCommandSigningInfo(self._keyChain, self._keyChain.getDefaultCertificateName())
		self._memoryContentCache = MemoryContentCache(self._face)

		self.lastRefreshTime = int(time.time())
		dataNode = self.boost.getDataNode()
		childrenNode = self.boost.getChildrenNode()

		print(self.boost.getNodePrefix())

		self._memoryContentCache.registerPrefix(Name(self.boost.getNodePrefix()), self.onRegisterFailed, self.onDataNotFound)
		#self._face.registerPrefix(Name(self.boost.getNodePrefix()), self.onInterest, self.onRegisterFailed)

		# For each type of data, we refresh each type of aggregation according to the interval in the configuration
		for i in range(len(dataNode.subtrees)):
			dataType = dataNode.subtrees.keys()[i]
			aggregationParams = self.boost.getProducingParamsForAggregationType(dataNode.subtrees.items()[i][1])

			if childrenNode == None:
				self._dataQueue[dataType] = DataQueue(None)
				self.generateData(dataType, 2, 0)

			for aggregationType in aggregationParams:
				childrenList = OrderedDict()
				if childrenNode != None:

					for j in range(len(childrenNode.subtrees)):
						if dataType in childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees:
							if aggregationType in childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees[dataType].subtrees:
								childrenList[childrenNode.subtrees.items()[j][0]] = self.boost.getProducingParamsForAggregationType(childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees[dataType])[aggregationType]
								#print("add child: " + childrenNode.subtrees.items()[j][0] + "-" + dataType + "-" + aggregationType)
				self.startPublishingAggregation(aggregationParams[aggregationType], childrenList, dataType, aggregationType)
		return

	def startPublishingAggregation(self, params, childrenList, dataType, aggregationType):
		if __debug__:
			print('Start publishing for ' + dataType + '-' + aggregationType)

		if len(childrenList.keys()) == 0:
			pass
		else:
			# express interest for children who produce the same data and aggregation type
			for child in childrenList.keys():
				name = Name(self.boost.getNodePrefix()).append(child).append('data').append(dataType).append('aggregation').append(aggregationType)
				interest = Interest(name)
				if ('start_time' in childrenList[child]):
					endTime = int(childrenList[child]['start_time']) + int(childrenList[child]['producer_interval'])
					interest.getName().append(str(childrenList[child]['start_time'])).append(str(endTime))
				else:
					interest.setChildSelector(1)
				interest.setInterestLifetimeMilliseconds(DEFAULT_INTEREST_LIFETIME)
				if __debug__:
					print('  Issue interest: ' + interest.getName().toUri())
				self._face.expressInterest(interest, self.onData, self.onTimeout)

		# aggregation calculating and publishing mechanism
		publishingPrefix = Name(self.boost.getNodePrefix()).append('data').append(dataType).append('aggregation').append(aggregationType)
		self._dataQueue[dataType + aggregationType] = DataQueue(params)
		self.calculateAggregation(dataType, aggregationType, childrenList, int(params['start_time']), int(params['producer_interval']), publishingPrefix)

		return

	# TODO: once one calculation's decided a child has not answered, we don't do another calculation
	def calculateAggregation(self, dataType, aggregationType, childrenList, startTime, interval, publishingPrefix):
		doCalc = True
		dataList = []

		# TODO: an intermediate node cannot produce raw data for now
		if len(childrenList.keys()) != 0:
			for child in childrenList.keys():
				dataDictKey = str(startTime) + '/' + str(startTime + interval) + '/' + child
				#print(dataDictKey)
				if dataDictKey in self._dataQueue[dataType + aggregationType]._dataDict:
					data = self._dataQueue[dataType + aggregationType]._dataDict[dataDictKey]
					dataList.append(int(data.getContent().toRawStr()))
					#print('Appended ' + child + ' to data list')
				else:
					#print('Child ' + child + ' has not replied yet')
					doCalc = False
					break
		else:
			for inst in self._dataQueue[dataType]._dataDict.keys():
				if int(inst) >= startTime - interval and int(inst) < startTime:
					dataList.append(self._dataQueue[dataType]._dataDict[inst])
		if doCalc:
			content = self._aggregation.getAggregation(aggregationType, dataList)
			if content:
				publishData = Data(Name(publishingPrefix).append(str(startTime - interval)).append(str(startTime)))
				publishData.setContent(str(content))
				publishData.getMetaInfo().setFreshnessPeriod(DEFAULT_DATA_LIFETIME)
				self._memoryContentCache.add(publishData)
				if __debug__:
					print("Produced: " + publishData.getName().toUri() + "; " + publishData.getContent().toRawStr())

		self._loop.call_later(interval, self.calculateAggregation, dataType, aggregationType, childrenList, startTime + interval, interval, publishingPrefix)
		return

	def generateData(self, dataType, interval, startTime):
		self._dataQueue[dataType]._dataDict[str(startTime)] = random.randint(0,9)
		self._loop.call_later(interval, self.generateData, dataType, interval, startTime + interval)
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
		dataName = data.getName()
		if __debug__:
			print("Got data: " + dataName.toUri() + "; " + data.getContent().toRawStr())
		for i in range(0, len(dataName)):
			if dataName.get(i).toEscapedString() == 'aggregation':
				dataAndAggregationType = dataName.get(i - 1).toEscapedString() + dataName.get(i + 1).toEscapedString()
				# This creation of dataDictKey means parent and child should not have the same name
				dataDictKey = dataName.get(i + 2).toEscapedString() + '/' + dataName.get(i + 3).toEscapedString() + '/' + dataName.get(i - 3).toEscapedString()
				print(dataDictKey)
				self._dataQueue[dataAndAggregationType]._dataDict[dataDictKey] = data
		return

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

	


