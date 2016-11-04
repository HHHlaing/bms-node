import time

from pyndn import Face, Name, Data, KeyLocator, Interest
from pyndn.security import KeyChain
#from pyndn.node import Node

def onRegisterSuccess(prefix, registeredPrefixId):
    print "Registered: " + prefix.toUri()

def onRegisterFailed(prefix):
    print "Prefix registration failed: " + prefix.toUri()

def onInterest(prefix, interest, face, interestFilterId, filter):
    print "Got interest " + interest.getName().toUri()

face = Face("128.97.98.7", 6363)
print "Local face is: " + str(face.isLocal())

keyChain = KeyChain()
face.setCommandSigningInfo(keyChain, keyChain._identityManager.getDefaultCertificateNameForIdentity(Name("/ndn/edu/ucla/remap/%40GUEST/wangzhehao410305%40gmail.com")))
face.registerPrefix("/ndn/edu/ucla/remap/zhehao", onInterest, onRegisterFailed, onRegisterSuccess)

while True:
    face.processEvents()
    # We need to sleep for a few milliseconds so we don't use 100% of the CPU.
    time.sleep(0.01)
