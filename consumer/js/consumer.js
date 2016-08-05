// consumer.js
//
//  Copyright 2015 Regents of the University of California
//  For licensing details see the LICENSE file.
//
//  Author:  Zhehao Wang <zhehao@remap.ucla.edu>

var Consumer = function(face, keyChain, root, displayCallback)
{
  this.face = face;
  this.prefix = new Name(root);
  this.keyChain = keyChain;
  
  // this records the currently being fetched spaces.
  this.activeSpace = [];
  
  this.defaultInterestLifetime = 3000;

  // Display callback is called once track data is received
  this.displayCallback = displayCallback;
};

Consumer.prototype.startFetchSpace = function(spaceName, dataType, aggregationType, startTime, interval)
{
  var interest = new Interest((new Name(this.prefix)).append(new Name(spaceName)));
  if (dataType === undefined) {
    throw "start fetch with undefined data type is not yet implemented!";
  } else if (dataType === '') {
    // TODO: make sure this is indeed the name for unlisted data type
    dataType = "unknown";
  }
  if (aggregationType === undefined || aggregationType === '') {
    interest.getName().append("data");
  } else {
    interest.getName().append("data").append(dataType).append("aggregation").append(aggregationType);
  }
  if (startTime !== undefined) {
    throw "start fetch with specific timestamp is not implemented!";
  } else {
    interest.setChildSelector(0);
  }
  interest.setMustBeFresh(true);
  interest.setInterestLifetimeMilliseconds(this.defaultInterestLifetime);
  this.face.expressInterest(interest, this.onData.bind(this), this.onTimeout.bind(this));
}

Consumer.prototype.onVerified = function()
{
  console.log("Data verify failed");
}

Consumer.prototype.onVerifyFailed = function()
{
  console.log("Data verified");
}

// Expected data name example: /ndn/edu/ucla/remap/bms/ucla/schoenberg/b420/stm.flw/data/SteamFlow/aggregation/avg
Consumer.prototype.onData = function(interest, data)
{
  this.keyChain.verifyData(data, this.onVerified, this.onVerifyFailed);
  
  var dataName = data.getName();
  console.log(dataName.toUri() + " " + data.getContent().buf().toString('binary'));
  
  var dataType = "";
  var aggregationType = "";
  var startTime = 0;
  var endTime = 0;

  for (var i = 0; i < dataName.size(); i++) {
    if (dataName.get(i).toEscapedString() == "aggregation") {
      var dataType = dataName.get(i - 1).toEscapedString();
      var aggregationType = dataName.get(i + 1).toEscapedString();
      
      var startTime = parseInt(dataName.get(i + 2).toEscapedString());
      var endTime = parseInt(dataName.get(i + 3).toEscapedString());
      break;
    }
  }

  if (aggregationType !== "" && dataType !== "" && startTime !== 0 && endTime !== 0) {
    // aggregation data
    this.displayCallback(dataName.getPrefix(i - 2).toUri(), dataType, aggregationType, startTime, endTime, data.getContent().buf().toString('binary'));

    var newInterestName = dataName.getPrefix(i + 2);
    newInterest = new Interest(interest);
    newInterest.setName(newInterestName);
    newInterest.setChildSelector(0);

    var exclude = new Exclude();
    exclude.appendAny();
    exclude.appendComponent(dataName.get(i + 2));
    newInterest.setExclude(exclude);

    this.face.expressInterest(newInterest, this.onData.bind(this), this.onTimeout.bind(this));
  } else {
    // raw data
    var newInterestName = new Name(data.getName()).getPrefix(-1);
    var newInterest = new Interest(interest);
    newInterest.setName(newInterestName);
    newInterest.setChildSelector(0);

    var exclude = new Exclude();
    exclude.appendAny();
    exclude.appendComponent(data.getName().get(-1));
    newInterest.setExclude(exclude);
    newInterest.refreshNonce();

    this.face.expressInterest(newInterest, this.onData.bind(this), this.onTimeout.bind(this));
  }

  return;
};

Consumer.prototype.onTimeout = function(interest)
{
  console.log("onTimeout called: " + interest.getName().toUri());
  this.face.expressInterest(interest, this.onData.bind(this), this.onTimeout.bind(this));
};