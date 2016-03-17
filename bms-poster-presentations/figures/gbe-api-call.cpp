// Producer
...
string prefix = 
  "/ndn/app/bms/";
string dataType = 
  "Electricity/ucla/Melnitz/1469A/xfmr-6.dmd.inst/data/";
// call construct to come up with E-key names to fetch when construct

DataProducer producer(Name(prefix), Name(dataType), ...);
// collect E-keys (Inferred E key names?) and create encrypted content keys
auto encryptedContentKeys =
  producer.createContentKey(“20150825T000000”, onCreated);
...
// onCreated: Publish encrypted content keys
Data data(prefix + "Aggregation/avg/20150825T000000/20150825T001000");
producer.produce(data, “20150825T000000”,
                 content, contentLen, onProduced);


// Group manager
...
GroupManager manager
  (prefix, "Electricity/ucla/Melnitz/1469A/");
// userCert1: cert of the group 
//   "/ndn/app/bms/read/ucla/Melnitz/1469A/"
manager.addMember(userCert1, schedule1);
auto groupKey = manager.getGroupKey(“20150825T000000”);
// publish group key

// Consumer


/*

The eventual data name is going to look like
prefix + data type + given time, for now, without suffix

Adding 2 and 3 as flat members of 1, or chaining them as 2 is a member of 1, and 3 is a member of 2
1. read/a/b/type
2. read/type/a/b
3. read/type/a
4. read/type

Consider using a different secondary group prefix, to show that secondary groups do not need to share the same prefix as the primary groups
"read" implies that things that come after have a hierarchical relationship

How would you define a secondary group? I would define it as a group that is a member of a group

*/