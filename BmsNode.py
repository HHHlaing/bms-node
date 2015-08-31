import time
import getopt
from collections import OrderedDict
from pyndn import Name
from pyndn import Face
from pyndn import Data
from pyndn.security import KeyChain
from pyndn.util.common import Common
from config_split import BoostInfoParser

class Aggregation(object):
	def __init__(self):
		pass

	def _doRefresh(self, aggregationType, *dataList):
		if len(dataList) == 0:
			assert False, 'DataList is None'
		if aggregationType == 'avg':
			return self.getAvg(dataList)
		elif aggregation == 'min':
			return self.getMin(dataList)
		elif aggregation == 'max':
			return self.getMax(dataList)
		else:
			assert False, 'Not be implemented'

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
		self._responseCount = None
		self._callbackCount = None
		self._tempName = None
		self._tempData = None
		self._dataQueue = []
		self.aggregation = Aggregation()

	def setConfiguration(self, fileName):
		self.boost = BoostInfoParser()
		self.boost.read(fileName)

	def doRefresh(self):
		time = self.lastRefreshTime
		self.lastRefreshTime = int(time.time())
		for i in range(len(self.boost.subtrees['data'].subtrees))
			dataType = self.boost.getProducerInterval(self.boost.subtrees['data'].subtrees.items()[i][0])
			refreshInterval = self.boost.getProducerInterval(self.boost.subtrees['data'].subtrees.items()[i][1])
			for j in range(len(refreshInterval)):
				self._doRefresh(time, refreshInterval.items()[j][1], dataType, refreshInterval.items()[j][0])

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
			else: #aggregation in ["avg", "min", "max"]:
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

	def registerPrefix(self):
		face = Face()
		self._keyChain = Keychain()
		self._certificateName = self._keyChain.getDefaultCertificateName()
		face.setCommandSigningInfo(self._keyChain, self._certificateName)

		self._responseCount = 0
		prefix = self.boost.getName()
		face.registerPrefix(prefix, onInterest, onRegisterFailed)

		return face

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
		self._responseCount += 1
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
		self._responseCount += 1

		raise RuntimeError("Register failed for prefix", prefix.toUri())

	def onData(self, interest, data):
		self._callbackCount += 1
		string = data.getContent().toRawStr()
		self._tempData = int(string)

	def onTimeout(self, interest):
		self._callbackCount += 1



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
			print
		else:
			assert False, "unhandled option"

main()

	


