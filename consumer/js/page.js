var consumer;

var startTimeOffset = 0;
var graphStartTime = 0;
var fetchDict;

$(document).ready(function(){
  var face = new Face("localhost");
  var prefix = "/ndn/edu/ucla/remap/bms";

  var identityStorage = new IndexedDbIdentityStorage();
  var privateKeyStorage = new IndexedDbPrivateKeyStorage();
  var policyManager = new NoVerifyPolicyManager();

  var keyChain = new KeyChain
    (new IdentityManager(identityStorage, privateKeyStorage),
     policyManager);
  keyChain.setFace(this.face);
  //face.setCommandSigningInfo(keyChain, self.certificateName);

  consumer = new Consumer(face, keyChain, prefix, displayCallback);
  startConsumer();
});

function startConsumer()
{
  // Hardcoded consumer with what to track
  fetchDict = {
                "/ndn/edu/ucla/remap/bms/ucla": 
                  {"spaceName": "ucla", "dataType": "SteamFlow", "aggregationType": "avg", "timeSeries": new TimeSeries()},
                "/ndn/edu/ucla/remap/bms/ucla/schoenberg": 
                  {"spaceName": "ucla/schoenberg", "dataType": "SteamFlow", "aggregationType": "avg", "timeSeries": new TimeSeries()},
                "/ndn/edu/ucla/remap/bms/ucla/schoenberg/b420":
                  {"spaceName": "ucla/schoenberg/b420", "dataType": "SteamFlow", "aggregationType": "avg", "timeSeries": new TimeSeries()},
                "/ndn/edu/ucla/remap/bms/ucla/factor":
                  {"spaceName": "ucla/factor", "dataType": "SteamFlow", "aggregationType": "avg", "timeSeries": new TimeSeries()},
                "/ndn/edu/ucla/remap/bms/ucla/factor/a-937a":
                  {"spaceName": "ucla/factor/a-937a", "dataType": "SteamFlow", "aggregationType": "avg", "timeSeries": new TimeSeries()}
              };

  for (var item in fetchDict) {
    consumer.startFetchSpace(fetchDict[item].spaceName, fetchDict[item].dataType, fetchDict[item].aggregationType);

    var chart = new SmoothieChart({timestampFormatter:SmoothieChart.timeFormatter});
    chart.addTimeSeries(fetchDict[item].timeSeries, { strokeStyle: 'rgba(0, 255, 0, 1)', fillStyle: 'rgba(0, 255, 0, 0.2)', lineWidth: 4 });
    chart.streamTo(document.getElementById(fetchDict[item].spaceName), 500);
  }
}

function displayCallback(dataName, dataType, aggregationType, startTime, endTime, dataContent)
{
  if (startTimeOffset === 0) {
    startTimeOffset = startTime;
    graphStartTime = new Date().getTime();
  }
  console.log("Callback called with " + dataName + " " + dataType + " " + aggregationType + " " + startTime.toString() + " " + endTime.toString());
  fetchDict[dataName].timeSeries.append(graphStartTime + (startTime - startTimeOffset), parseFloat(dataContent));
}