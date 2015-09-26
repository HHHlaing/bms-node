import tailer 
import parse 
import argparse
import sys
import json
import logging
from datetime import datetime

from base64 import b64decode, b64encode

import subprocess
import time
import select

import csv

try:
    import asyncio
except ImportError:
    import trollius as asyncio
    from concurrent.futures import ProcessPoolExecutor

from pyndn import Name, Data, KeyLocator
from pyndn.threadsafe_face import ThreadsafeFace
from pyndn.util.memory_content_cache import MemoryContentCache
from pyndn.util import Blob

from pyndn.security import KeyChain
from pyndn.security.identity.file_private_key_storage import FilePrivateKeyStorage
from pyndn.security.identity.basic_identity_storage import BasicIdentityStorage
from pyndn.security.identity.identity_manager import IdentityManager
from pyndn.security.policy.config_policy_manager import ConfigPolicyManager

import subprocess

DO_CERT_SETUP = True

# Syntax for Python 2, quick hack for getting everyone signed
if DO_CERT_SETUP:                    
    import urllib2


# Value of dictionary _sensorNDNDict
#       key:    string, string name of sensor from given csv and received sensor data JSON
#       value:  _aggregationName, Name, the ndn-ized aggregation data name prefix
#               _instName, Name, the ndn-ized instantaneous data name
class SensorNDNDictItem(object):
    def __init__(self, aggregationName, instName):
        self._aggregationName = aggregationName
        self._instName = instName

class DataQueueItem(object):
    def __init__(self, dataList, timeThreshold, identityName, certificateName):
        self._dataList = dataList
        self._timeThreshold = timeThreshold
        self._identityName = identityName
        self._certificateName = certificateName

class DataPublisher(object):
    def __init__(self, face, keyChain, loop, cache, namespace):
        # Start time of this instance
        self._startTime = 0
        # Default aggregation interval in seconds
        self._defaultInterval = 10000
        # Dictionary that holds the temporary data to calculate aggregation with
        # Key   - sensor name
        # Value - data list: [], list of sensor data
        #         timeThreshold: int, any data before this timestamp should be used for aggregation calculation; 
        #                             here we assume for each sensor, its data would come in order on this node
        self._dataQueue = dict()
        self._face = face
        self._keyChain = keyChain
        self._loop = loop
        self._cache = cache
        self._namespace = namespace
        self.DEFAULT_DATA_LIFETIME = 2000000

        self._sensorNDNDict = dict()

    def populateSensorNDNDictFromCSV(self, csvFileName):
        with open(csvFileName, 'rU') as csvFile:
            reader = csv.reader(csvFile, delimiter=',', quotechar='|')

            campusComponentName = "ucla"
            for row in reader:
                if (len(row)) > 5:
                    # sensor full name, building name, room name, sensor name, sensor data type
                    #print(row[1], row[2], row[3], row[4], row[5])
                    key = ''
                    dataType = "unknown_data_type";
                    if (row[5] != ''):
                        dataType = row[5]
                    if (row[3] != ''):
                        key = row[2].lower().strip() + '.' + row[3].lower().strip() + '.' + row[4].lower().strip()
                        ndnNameString = campusComponentName + '/' + row[2].lower().strip() + '/' + row[3].lower().strip() + '/' + row[4].lower().strip()
                        aggregationName = Name(ndnNameString).append('data').append(dataType).append('aggregation')
                        instName = Name(ndnNameString).append('data').append(dataType).append('inst')

                        self._sensorNDNDict[key] = SensorNDNDictItem(aggregationName, instName)
                    else:
                        key = row[2].lower().strip() + '.' + row[4].lower().strip()
                        ndnNameString = campusComponentName + '/' + row[2].lower().strip() + '/' + row[4].lower().strip()
                        aggregationName = Name(ndnNameString).append('data').append(dataType).append('aggregation')
                        instName = Name(ndnNameString).append('data').append(dataType).append('inst')

                        self._sensorNDNDict[key] = SensorNDNDictItem(aggregationName, instName)

    def publish(self, line):
        # Pull out and parse datetime for log entry 
        # (note we shoudld use point time for timestamp)
        try:
            if not ": (point" in line: return
            point = parse.search("(point {})", line)[0].split(" ")
        except Exception as detail:
            print("publish: Parse error for", line, "-", detail)
            return
        try:
            tempTime = datetime.strptime(parse.search("[{}]", line)[0], "%Y-%m-%d %H:%M:%S.%f")
        except Exception as detail:
            print("publish: Date/time conversion error for", line, "-", detail)
            return
            
        sensorName = point[0]
        aggregationNamePrefix = self.pointNameToNDNName(sensorName)
        dataDict = self.pointToJSON(point)
        
        if aggregationNamePrefix is not None:
            #if __debug__:
            #    print(dateTime, aggregationNamePrefix, dataDict["timestamp"], "payload:", dataDict["value"])
            try:
                # TODO: since the leaf sensor publisher is not a separate node for now, we also publish aggregated data
                #       of the same sensor over the past given time period in this code;
                #       bms_node code has adaptation for leaf sensor publishers as well, ref: example-sensor1.conf

                # Here we make the assumption of fixed time window for *all* sensors
                # First publish aggregation
                dataTime = int(float(dataDict["timestamp"]) * 1000)
                if self._startTime == 0:
                    self._startTime = dataTime
                if not (sensorName in self._dataQueue):
                    # We don't have record of this sensor, so we create an identity for it, and print the cert string for now to get signed
                    sensorIdentityName = Name(self._namespace).append(aggregationNamePrefix).getPrefix(-3)
                    sensorCertificateName = self._keyChain.createIdentityAndCertificate(sensorIdentityName)
                    if __debug__:
                        print("Sensor identity name: " + sensorIdentityName.toUri())
                    certificateData = self._keyChain.getIdentityManager()._identityStorage.getCertificate(sensorCertificateName, True)

                    # We should only ask for cert to be signed upon the first run of a certain sensor
                    if DO_CERT_SETUP:
                        if (KeyLocator.getFromSignature(certificateData.getSignature()).getKeyName().equals(sensorCertificateName.getPrefix(-1))):
                            # Need to configure for remote.
                            print("certificate " + sensorCertificateName.toUri() + " asking for signature")
                            response = urllib2.urlopen("http://192.168.56.1:5000/bms-cert-hack?cert=" + b64encode(certificateData.wireEncode().toBuffer()) + "&cert_prefix=" + sensorIdentityName.toUri() + '&subject_name=' + sensorIdentityName.toUri()).read()
                            
                            signedCertData = Data()
                            signedCertData.wireDecode(Blob(b64decode(response)))

                            self._cache.add(signedCertData)
                            cmdline = ['ndnsec-install-cert', '-']
                            p = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                            cert, err = p.communicate(response)
                            if p.returncode != 0:
                                raise RuntimeError("ndnsec-install-cert error")
                        else:
                            self._cache.add(certificateData)
                    else:
                        self._cache.add(certificateData)

                    self._dataQueue[sensorName] = DataQueueItem([], self._startTime + self._defaultInterval, sensorIdentityName, sensorCertificateName)
                    self._dataQueue[sensorName]._dataList.append(dataDict["value"])
                elif dataTime > self._dataQueue[sensorName]._timeThreshold:
                    # calculate the aggregation with what's already in the queue, publish data packet, and delete current queue
                    # TODO: This should be mutex locked against self
                    if len(self._dataQueue[sensorName]._dataList) > 0:
                        avg = 0.0
                        for item in self._dataQueue[sensorName]._dataList:
                            avg += float(item)
                        avg = avg / len(self._dataQueue[sensorName]._dataList)
                        data = Data(Name(self._namespace).append(aggregationNamePrefix).append("avg").append(str(self._dataQueue[sensorName]._timeThreshold)).append(str(self._dataQueue[sensorName]._timeThreshold + self._defaultInterval)))
                        data.setContent(str(avg))
                        data.getMetaInfo().setFreshnessPeriod(self.DEFAULT_DATA_LIFETIME)
                        self._keyChain.sign(data, self._dataQueue[sensorName]._certificateName)
                        self._cache.add(data)
                        print("Aggregation produced " + data.getName().toUri())

                    self._dataQueue[sensorName]._dataList = [dataDict["value"]]
                    self._dataQueue[sensorName]._timeThreshold = self._dataQueue[sensorName]._timeThreshold + self._defaultInterval
                else:
                    self._dataQueue[sensorName]._dataList.append(dataDict["value"])
                
                # Then publish raw data
                # Timestamp in data name uses the timestamp from data payload
                instDataPrefix = self.pointNameToNDNName(sensorName, False)
                dataTemp = self.createData(instDataPrefix, dataDict["timestamp"], dataDict["value"], self._dataQueue[sensorName]._certificateName)
                if __debug__:
                    print("Produced raw data name " + dataTemp.getName().toUri())
                    print("Produced raw data content " + dataTemp.getContent().toRawStr())
                self._cache.add(dataTemp)

            except Exception as detail:
                print("publish: Error calling createData for", line, "-", detail)

    def createData(self, namePrefix, timestamp, payload, certName):
        data = Data(Name(self._namespace).append(namePrefix).append(str(timestamp)))
        data.setContent(payload)
        self._keyChain.sign(data, certName)
        data.getMetaInfo().setFreshnessPeriod(self.DEFAULT_DATA_LIFETIME)
        if __debug__:
            print(data.getName().toUri())
        return data
    
    # @param {Boolean} isAggregation, True for return sensorName/data/aggregation/type, False for return sensorName/data/raw/type
    # @return {NDN name | None} None if asked for aggregation and string name entry is not found; NDN name is string entry is found, or can be inferred
    #          Aggregation name looks like /ndn/app/bms/ucla/boelter/data/ElectricityDemand/aggregation/avg/140003200/140003210
    def pointNameToNDNName(self, point, isAggregation = True):
        name = point.lower().split(":")[1]
        
        # We expect failures in name transformation to be as little as possible; for our current historical data, 3 out of ~800 sensor names cannot be found
        if (name in self._sensorNDNDict):
            if isAggregation:
                return self._sensorNDNDict[name]._aggregationName
            else:
                return self._sensorNDNDict[name]._instName
        else:
            print("Sensor name " + name + " not found in dict, aggregation calculation skipped; raw publishing with assumed name")
            if isAggregation:
                return None
            else:
                return Name(name.replace('.', '/'))


    def pointToJSON(self, pd):
        d = {}
        args = ["pointname", "type", "value", "conf", "security", "locked", "seconds", "nanoseconds", "unknown_1", "unknown_2"]
        for i in range(len(args)):
            try:
                d[args[i]] = pd[i]
            except Exception as detail:
                d[args[i]] = None
                print("pointToJSON: Error parsing arg", args[i], "from", pd, "-", detail)
        try:
            timestamp = (int(d["seconds"]) + int(d["nanoseconds"])*1e-9)
            dt = datetime.fromtimestamp(timestamp)
            d["timestamp_str"] = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            d["timestamp"] = str(timestamp)
        except Exception as detail:
            print("pointToJSON: Error in timestamp conversation of", pd)
            d["timestamp"] = 0
            d["timestamp_str"] = ("0000-00-00 00:00:00.00")
        try:
            if __debug__:
                print(json.dumps(d))
        except Exception as detail:
            print("pointToJSON: Error in JSON conversation of", pd)
            return "{}"
        return d

    @asyncio.coroutine
    def readfile(self, filename):
        f = open(filename, 'r')
        for line in f:
            self.publish(line)
            yield None
        f.close()
        
    @asyncio.coroutine
    def followfile(self, filename):
        f = subprocess.Popen(['tail','-F', filename],\
              stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        p = select.poll()
        p.register(f.stdout)

        while True:
            if p.poll(1):
                self.publish(f.stdout.readline())
            time.sleep(0.01)
            yield None

        #for line in tailer.follow(open(filename)):
        #    publish(line, namespace, cache)

    def onRegisterFailed(self, prefix):
        print("register failed for " + prefix.getName().toUri())
        raise RuntimeError("Register failed for prefix", prefix.toUri())

    def onDataNotFound(self, prefix, interest, face, interestFilterId, filter):
        print('DataNotFound : ' + interest.getName().toUri())
        return

class Logger(object):
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
    # Params parsing
    parser = argparse.ArgumentParser(description='bms gateway node to Parse or follow Cascade Datahub log and publish to MiniNdn.')
    parser.add_argument('filename', help='datahub log file')
    parser.add_argument('-f', dest='follow', action='store_true', help='follow (tail -f) the log file')  
    parser.add_argument('--namespace', default='/ndn/edu/ucla/remap/bms', help='root of ndn name, no trailing slash')
    args = parser.parse_args()
    
    # Setup logging
    logger = Logger()
    logger.prepareLogging()

    # Face, KeyChain, memoryContentCache and asio event loop initialization
    loop = asyncio.get_event_loop()
    face = ThreadsafeFace(loop)

    keyChain = KeyChain(IdentityManager(BasicIdentityStorage(), FilePrivateKeyStorage()))
    # For the gateway publisher, we create one identity for it to sign nfd command interests
    certificateName = keyChain.createIdentityAndCertificate(Name("/ndn/bms/gateway-publisher"))
    face.setCommandSigningInfo(keyChain, certificateName)
    cache = MemoryContentCache(face)

    dataPublisher = DataPublisher(face, keyChain, loop, cache, args.namespace)
    cache.registerPrefix(Name(args.namespace), dataPublisher.onRegisterFailed, dataPublisher.onDataNotFound)
    
    # Parse csv to decide the mapping between sensor JSON -> <NDN name, data type>
    dataPublisher.populateSensorNDNDictFromCSV('bms-sensor-data-types-sanitized.csv')

    if args.follow: 
        #asyncio.async(loop.run_in_executor(executor, followfile, args.filename, args.namespace, cache))
        loop.run_until_complete(dataPublisher.followfile(args.filename))
    else:
        loop.run_until_complete(dataPublisher.readfile(args.filename))
        
    loop.run_forever()
    face.shutdown()
        
if __name__ == '__main__':
    main()
