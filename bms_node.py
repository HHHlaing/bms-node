import time
import getopt
import sys
import logging
import random

from base64 import b64decode, b64encode

from collections import OrderedDict
from pyndn import Name, Data, Interest, Exclude, KeyLocator
from pyndn.threadsafe_face import ThreadsafeFace

from pyndn.security import KeyChain
from pyndn.security.identity.file_private_key_storage import FilePrivateKeyStorage
from pyndn.security.identity.basic_identity_storage import BasicIdentityStorage
from pyndn.security.identity.identity_manager import IdentityManager
from pyndn.security.policy.config_policy_manager import ConfigPolicyManager

from pyndn.util.common import Common
from pyndn.util import MemoryContentCache, Blob

try:
    import asyncio
except ImportError:
    import trollius as asyncio

from config_split import BoostInfoParser

# TODO: start aggregation with rightMostChild mode, non-mandatory child's data

# Program constants
DEFAULT_INTEREST_LIFETIME = 2000
DEFAULT_DATA_LIFETIME = 2000000

# Namespace constants
DATA_COMPONENT = "data"
AGGREGATION_COMPONENT = "aggregation"

DO_CERT_SETUP = True

# Syntax for Python 2, quick hack for getting everyone signed
if DO_CERT_SETUP:                    
    import urllib2

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

# For each data and aggregation type, a data queue with a dictionary, 
# publishing params, list of children and publishingPrefix is created; 
class DataQueue(object):
    def __init__(self, publishingParams, childrenList, publishingPrefix):
        self._dataDict = dict()
        self._publishingParams = publishingParams
        self._childrenList = childrenList
        self._publishingPrefix = publishingPrefix
        return

class BmsNode(object):
    def __init__(self):
        self.conf = None
        self._keyChain = None
        self._certificateName = None

        self._dataQueue = dict()
        self._memoryContentCache = None
        self._identityName = None

        self._aggregation = Aggregation()

    def setConfiguration(self, fileName, trustSchemaFile):
        self.conf = BoostInfoParser()
        self.conf.read(fileName)
        self._identityName = Name(self.conf.getNodePrefix())
        self._trustSchemaFile = trustSchemaFile

    def onDataNotFound(self, prefix, interest, face, interestFilterId, filter):
        #print('Data not found for ' + interest.getName().toUri())
        return

    def startPublishing(self):
        # One-time security setup
        self.prepareLogging()

        privateKeyStorage = FilePrivateKeyStorage()
        identityStorage = BasicIdentityStorage()
        policyManager = ConfigPolicyManager(self._trustSchemaFile)

        self._keyChain = KeyChain(IdentityManager(identityStorage, privateKeyStorage), policyManager)
        self._certificateName = self._keyChain.createIdentityAndCertificate(self._identityName)

        print("My Identity name: " + self._identityName.toUri())
        print("My certificate name: " + self._certificateName.toUri())
        certificateData = self._keyChain.getIdentityManager()._identityStorage.getCertificate(self._certificateName, True)
        print("My certificate string: " + b64encode(certificateData.wireEncode().toBuffer()))
        # self._keyChain.getIdentityCertificate(self._certificateName).)

        self._loop = asyncio.get_event_loop()
        self._face = ThreadsafeFace(self._loop)
        self._keyChain.setFace(self._face)

        self._face.setCommandSigningInfo(self._keyChain, self._certificateName)
        self._memoryContentCache = MemoryContentCache(self._face)

        # We should only ask for cert to be signed upon the first run of a certain aggregator
        if DO_CERT_SETUP:
            if (KeyLocator.getFromSignature(certificateData.getSignature()).getKeyName().equals(self._certificateName.getPrefix(-1))):
                # Need to configure for remote deployment.
                response = urllib2.urlopen("http://192.168.56.1:5000/bms-cert-hack?cert=" + b64encode(certificateData.wireEncode().toBuffer()) + "&cert_prefix=" + self._identityName.toUri() + '&subject_name=' + self._identityName.toUri()).read()
                
                signedCertData = Data()
                signedCertData.wireDecode(Blob(b64decode(response)))

                self._memoryContentCache.add(signedCertData)
            else:
                self._memoryContentCache.add(certificateData)
        else:
            self._memoryContentCache.add(certificateData)

        dataNode = self.conf.getDataNode()
        childrenNode = self.conf.getChildrenNode()

        self._memoryContentCache.registerPrefix(Name(self._identityName), self.onRegisterFailed, self.onDataNotFound)

        # For each type of data, we refresh each type of aggregation according to the interval in the configuration
        for i in range(len(dataNode.subtrees)):
            dataType = dataNode.subtrees.keys()[i]
            aggregationParams = self.conf.getProducingParamsForAggregationType(dataNode.subtrees.items()[i][1])

            if childrenNode == None:
                self._dataQueue[dataType] = DataQueue(None, None, None)
                self.generateData(dataType, 2, 0)

            for aggregationType in aggregationParams:
                childrenList = OrderedDict()
                if childrenNode != None:

                    for j in range(len(childrenNode.subtrees)):
                        if dataType in childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees:
                            if aggregationType in childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees[dataType].subtrees:
                                childrenList[childrenNode.subtrees.items()[j][0]] = self.conf.getProducingParamsForAggregationType(childrenNode.subtrees.items()[j][1].subtrees['data'].subtrees[dataType])[aggregationType]

                self.startPublishingAggregation(aggregationParams[aggregationType], childrenList, dataType, aggregationType)
        return

    def startPublishingAggregation(self, params, childrenList, dataType, aggregationType):
        if __debug__:
            print('Start publishing for ' + dataType + '-' + aggregationType)
        
        # aggregation calculating and publishing mechanism
        publishingPrefix = Name(self._identityName).append(DATA_COMPONENT).append(dataType).append(AGGREGATION_COMPONENT).append(aggregationType)
        self._dataQueue[dataType + aggregationType] = DataQueue(params, childrenList, publishingPrefix)

        if len(childrenList.keys()) == 0:
            # TODO: make start_time optional for leaf nodes
            self._loop.call_later(int(params['producer_interval']), self.calculateAggregation, dataType, aggregationType, childrenList, int(params['start_time']), int(params['producer_interval']), publishingPrefix, True)
        else:
            # express interest for children who produce the same data and aggregation type
            for childName in childrenList.keys():
                name = Name(self._identityName).append(childName).append(DATA_COMPONENT).append(dataType).append(AGGREGATION_COMPONENT).append(aggregationType)
                interest = Interest(name)
                # if start_time is specified, we ask for data starting at start_time; 
                # if not, we ask for the right most child and go from there
                if ('start_time' in childrenList[childName]):
                    endTime = int(childrenList[childName]['start_time']) + int(childrenList[childName]['producer_interval'])
                    interest.getName().append(str(childrenList[childName]['start_time'])).append(str(endTime))
                else:
                    # TODO: For now we are playing with historical data, for each run we don't want to miss any data, thus we start with leftMost
                    interest.setChildSelector(0)
                    interest.setMustBeFresh(True)
                interest.setInterestLifetimeMilliseconds(DEFAULT_INTEREST_LIFETIME)
                if __debug__:
                    print('  Issue interest: ' + interest.getName().toUri())
                self._face.expressInterest(interest, self.onData, self.onTimeout)

        return

    # TODO: once one calculation's decided a child has not answered, we should do another calculation
    def calculateAggregation(self, dataType, aggregationType, childrenList, startTime, interval, publishingPrefix, repeat = False):
        doCalc = True
        dataList = []

        # TODO: an intermediate node cannot produce raw data for now
        if len(childrenList.keys()) != 0:
            for childName in childrenList.keys():
                dataDictKey = self.getDataDictKey(startTime, (startTime + interval), childName)
                if dataDictKey in self._dataQueue[dataType + aggregationType]._dataDict:
                    data = self._dataQueue[dataType + aggregationType]._dataDict[dataDictKey]
                    dataList.append(float(data.getContent().toRawStr()))
                else:
                    #print('Child ' + childName + ' has not replied yet')
                    doCalc = False
                    break
        else:
            for inst in self._dataQueue[dataType]._dataDict.keys():
                if int(inst) >= startTime and int(inst) < startTime + interval:
                    dataList.append(self._dataQueue[dataType]._dataDict[inst])
        if doCalc:
            content = self._aggregation.getAggregation(aggregationType, dataList)
            if content:
                publishData = Data(Name(publishingPrefix).append(str(startTime)).append(str(startTime + interval)))
                publishData.setContent(str(content))
                publishData.getMetaInfo().setFreshnessPeriod(DEFAULT_DATA_LIFETIME)
                self._keyChain.sign(publishData, self._certificateName)
                self._memoryContentCache.add(publishData)
                for childName in childrenList.keys():
                    dataDictKey = self.getDataDictKey(startTime, (startTime + interval), childName)
                    if dataDictKey in self._dataQueue[dataType + aggregationType]._dataDict:
                        del self._dataQueue[dataType + aggregationType]._dataDict[dataDictKey]
                if __debug__:
                    print("Produced: " + publishData.getName().toUri() + "; " + publishData.getContent().toRawStr())

        # repetition of this function only happens for raw data producer, otherwise calculateAggregation is called by each onData
        if repeat:
            self._loop.call_later(interval, self.calculateAggregation, dataType, aggregationType, childrenList, startTime + interval, interval, publishingPrefix, repeat)
        return

    def generateData(self, dataType, interval, startTime):
        self._dataQueue[dataType]._dataDict[str(startTime)] = random.randint(0,9)
        self._loop.call_later(interval, self.generateData, dataType, interval, startTime + interval)
        return

    def onRegisterFailed(self, prefix):
        raise RuntimeError("Register failed for prefix", prefix.toUri())

    def onVerified(self, data):
        print('Data verified: ' + data.getName().toUri())
        return

    def onVerifyFailed(self, data):
        print('Data verification failed: ' + data.getName().toUri())
        return

    def onData(self, interest, data):
        self._keyChain.verifyData(data, self.onVerified, self.onVerifyFailed)

        dataName = data.getName()
        dataQueue = None

        if __debug__:
            print("Got data: " + dataName.toUri() + "; " + data.getContent().toRawStr())
        for i in range(0, len(dataName)):
            if dataName.get(i).toEscapedString() == AGGREGATION_COMPONENT:
                dataType = dataName.get(i - 1).toEscapedString()
                aggregationType = dataName.get(i + 1).toEscapedString()
                
                startTime = int(dataName.get(i + 2).toEscapedString())
                endTime = int(dataName.get(i + 3).toEscapedString())
                childName = dataName.get(i - 3).toEscapedString()

                dataAndAggregationType = dataType + aggregationType
                
                dataDictKey = self.getDataDictKey(startTime, endTime, childName)
                dataQueue = self._dataQueue[dataAndAggregationType]
                dataQueue._dataDict[dataDictKey] = data
                break

        # TODO: check what if interval/starttime is misconfigured
        if dataQueue:
            self.calculateAggregation(dataType, aggregationType, dataQueue._childrenList, startTime, endTime - startTime, dataQueue._publishingPrefix)

        # Always ask for the next piece of data when we receive this one; assumes interval does not change; this also assumes there are no more components after endTime
        #newInterestName = dataName.getPrefix(i + 2).append(str(endTime)).append(str(endTime + (endTime - startTime)))
        
        # We don't expect aggregated data name to be continuous within our given time window, so we ask with exclusion instead
        newInterestName = dataName.getPrefix(i + 2)
        newInterest = Interest(interest)
        newInterest.setName(newInterestName)
        newInterest.setChildSelector(0)

        exclude = Exclude()
        exclude.appendAny()
        exclude.appendComponent(dataName.get(i + 2))
        newInterest.setExclude(exclude)

        self._face.expressInterest(newInterest, self.onData, self.onTimeout)
        if __debug__:
            print("  issue interest: " + interest.getName().toUri())

        return

    def onTimeout(self, interest):
        if __debug__:
            print("  interest timeout: " + interest.getName().toUri() + "; reexpress")
            pass
        self._face.expressInterest(interest, self.onData, self.onTimeout)
        return

    def stop(self):
        self._loop.stop()
        if __debug__:
            print("Stopped")
        return
    
    # This creation of dataDictKey means parent and child should not have the same name            
    @staticmethod
    def getDataDictKey(startTime, endTime, childName):
        return str(startTime) + '/' + str(endTime) + '/' + childName

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

def usage():
    print("Usage: python bms_node.py --conf=[path to config file]")
    return

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["conf=", "schema="])
    except getopt.GetoptError as err:
        print err
        usage()
        sys.exit(2)
    if (len(opts) == 0):
        print("Error: missing required conf file")
        usage()
        sys.exit(2)

    confFile = "./confs/ucla.conf"
    schemaFile = "trust_schema.conf"
    for o, a in opts:
        if o == "--conf":
            confFile = a
        elif o == '--schema':
            schemaFile = a
        else:
            print("unhandled option")

    bNode = BmsNode()
    bNode.setConfiguration(confFile, schemaFile)
    bNode.startPublishing()

    try:
        bNode._loop.run_forever()
    except Exception as e:
        print(e)
    finally:
        bNode.stop()
main()

    


