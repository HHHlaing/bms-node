import tailer 
import parse 
import argparse
import sys
import json
import logging
from datetime import datetime

import subprocess
import time
import select

try:
    import asyncio
except ImportError:
    import trollius as asyncio
    from concurrent.futures import ProcessPoolExecutor

from pyndn import Name, Data
from pyndn.threadsafe_face import ThreadsafeFace
from pyndn.security import KeyChain
#from pyndn.security.identity import FilePrivateKeyStorage, BasicIdentityStorage
#from pyndn.security.identity import IdentityManager
from pyndn.util.memory_content_cache import MemoryContentCache

DEFAULT_DATA_LIFETIME = 2000000
T = datetime.strptime("2015-02-05", "%Y-%m-%d")

def publish(line, rootName, cache):
    global face, keyChain, T
    # Pull out and parse datetime for log entry 
    # (note we shoudld use point time for timestamp)
    try:
        if not ": (point" in line: return
        dateTimeStr = parse.search("[{}]", line)[0]
        point = parse.search("(point {})", line)[0].split(" ")
    except Exception as detail:
        print("publish: Parse error for", line, "-", detail)
        return
    try:
        dateTime = datetime.strptime(dateTimeStr, "%Y-%m-%d %H:%M:%S.%f")
    except Exception as detail:
        print("publish: Date/time conversion error for", line, "-", detail)
        return
        
    name = pointNameToName(point[0], rootName)
    dataDict = pointToJSON(point)
    
    if name is not None:
        #print("Publishing log entry", logdt, "to", name, dataDict["timestamp"], "payload:", dataJson)
        if dateTime < T: return
        print(dateTime, name, dataDict["timestamp"], "payload:", dataDict["value"])
        try:
            dataTemp = createData(name, dataDict["timestamp"], dataDict["value"])
            print(dataTemp.getName().toUri())
            print(dataTemp.getContent().toRawStr())
            cache.add(dataTemp)
            
        except Exception as detail:
            print("publish: Error calling createData for", line, "-", detail)

def createData(name, timestamp, payload):
    data = Data(Name(name + "/" + str(timestamp))) 
    data.setContent(payload)
    #keyChain.sign(data, keyChain.getDefaultCertificateName())
    data.getMetaInfo().setFreshnessPeriod(DEFAULT_DATA_LIFETIME)
    print("ndn:" + name + "/" + str(timestamp) + "\t:" + payload)
    return data

def pointNameToName(point, root):
    try: 
        comps = point.lower().split(":")[1].split(".")

        # If the number of comps is more than 4, the extra parts will be concated together with comps[3] as a new comps[3].
        if len(comps) > 4:
            extraComps = "-".join(comps[3:])
            name = root + "/" + "/".join(comps[0:3]) + "/" + extraComps
        else:
            name = root + "/" + "/".join(comps)
    except Exception as detail:
        print("publish: Error constructing name for", point, "-", detail)
        return None
    return name

def pointToJSON(pd):
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
        print(json.dumps(d))
    except Exception as detail:
        print("pointToJSON: Error in JSON conversation of", pd)
        return "{}"
    return d

#@asyncio.coroutine
def readfile(filename, namespace, cache):
    f = open(filename, 'r')
    for line in f:
        publish(line, namespace, cache)
    f.close()
    
@asyncio.coroutine
def followfile(filename, namespace, cache):
    f = subprocess.Popen(['tail','-F', filename],\
          stdout = subprocess.PIPE,stderr = subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)

    while True:
        if p.poll(1):
            publish(f.stdout.readline(), namespace, cache)
        time.sleep(0.01)
        yield None

    #for line in tailer.follow(open(filename)):
    #    publish(line, namespace, cache)

def onRegisterFailed(prefix):
    print("register failed for " + prefix.getName().toUri())
    raise RuntimeError("Register failed for prefix", prefix.toUri())

def onDataNotFound(prefix, interest, face, interestFilterId, filter):
    print('DataNotFound')
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
    parser = argparse.ArgumentParser(description='bms gateway node to Parse or follow Cascade Datahub log and publish to MiniNdn.')
    parser.add_argument('filename', help='datahub log file')
    parser.add_argument('-f', dest='follow', action='store_true', help='follow (tail -f) the log file')  
    parser.add_argument('--namespace', default='/ndn/edu/ucla/remap/bms', help='root of ndn name, no trailing slash')
    args = parser.parse_args()
   
    logger = Logger()
    logger.prepareLogging()

    #readfile(args.filename, args.namespace, None)
    #sys.exit(1)
    
    global face, keyChain
    loop = asyncio.get_event_loop()
    face = ThreadsafeFace(loop)

    #keychain = KeyChain(IdentityManager(BasicIdentityStorage(), FilePrivateKeyStorage()))
    keyChain = KeyChain()
    face.setCommandSigningInfo(keyChain, keyChain.getDefaultCertificateName())
    cache = MemoryContentCache(face)
    cache.registerPrefix(Name(args.namespace), onRegisterFailed, onDataNotFound)
    # READ THE FILE (MAIN LOOP)
    
    #executor = ProcessPoolExecutor(2)

    if args.follow: 
        #asyncio.async(loop.run_in_executor(executor, followfile, args.filename, args.namespace, cache))
        loop.run_until_complete(followfile(args.filename, args.namespace, cache))
    else:
        loop.run_until_complete(readfile(args.filename, args.namespace, cache))
    print("bah")
    loop.run_forever()
    face.shutdown()
        
if __name__ == '__main__':
    main()
