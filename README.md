Mini-BMS
======================================

Mini-bms is an update on the [NDN BMS](http://web.cs.ucla.edu/~lixia/papers/IEEENetMay14.pdf) work, which features sensor data aggregation by type and location, and an option to run in [mini-ndn](https://github.com/named-data/mini-ndn).

The design, implementation, and current deployment is explained in [this poster at NDNComm 2015](https://github.com/zhehaowang/bms-node/raw/master/bms-poster-presentations/ndn-bms-poster.pdf). A screenshot of the sample visualization unit is available [here](https://github.com/zhehaowang/bms-node/blob/master/bms-poster-presentations/figures/bms-aggregation-concept-screenshot.png).

Components
-------
* BMS publisher: gateway-publisher/bms\_publisher.py
  * Gateway publisher reads data from a file, parses the data, generates the NDN data packet and inserts into its own memory content cache. 
  * A sensor data entry in the input file looks like: 
<pre>
[2015-02-04 02:00:52.986]: 128:8: Process message (point UCLA:YOUNG_LIBRY.B1716.CHWS.RT 1 -522.84515380859375 0 0 0 1423044067 419999837 577 192)
</pre>
  * Publisher uses the result of csv\_reader.py to decide the mapping from sensor data entries to NDN names. csv\_reader.py reads bms-sensor-data-types-sanitized.csv to generate te mapping.
* BMS node: bms_node.py
  * BMS node reads a configuration file to decide its node name, which types of data this node should ask for, and what aggregated data this node should produce.
  * BMS node uses config\_split.py to parse the configuration file. Example configuration files are given in confs folder.
* BMS consumer: consumer/index.html
  * An in-browser NDN consumer and visualizer of the produced aggregated data.
* [BMS certification service](https://github.com/zhehaowang/openmhealth-cert/tree/bms-cert-hack): a quick hack of a BMS certificate issuing web service, so that the aggregation nodes and gateway publisher can have their certificates signed by BMS's root of trust. This service signs any certificates received. Consumer would then be able to verify the BMS data with the correct root of trust installed in certs/anchor.cert. This site is based on [ndncert](https://github.com/named-data/ndncert).

How to use
-------
* Start NFD on all nodes
* (Optional) Run the BMS certification service.
<pre>
  git clone https://github.com/zhehaowang/openmhealth-cert
  cd openmhealth-cert
  git checkout -b bms-cert-hack origin/bms-cert-hack
  cd www
  python ndncert-server.py
</pre>
  * Running the certification service requires Python Flask and MongoDB installed. In MongoDB there should be a "ndncert" database, with at least one entry in "operators" table that looks like the following. The prefix's corresponding public/private key should also be available in the system
<pre>
  { "_id" : "0", "site_prefix" : "/org/openmhealth", "site_name" : "dummy", "email" : "wangzhehao410305@gmail.com", "name" : "zhehao" }
</pre>
  * After the certification service's running, you may need to change the hardcoded address [here](https://github.com/zhehaowang/bms-node/blob/1ab0285c4a0d739514b429380d076f494b98660e/bms_node.py#L138) and [here](https://github.com/zhehaowang/bms-node/blob/1ab0285c4a0d739514b429380d076f494b98660e/gateway-publisher/bms_publisher.py#L153)
  * Dump the certificate that corresponds to the site\_prefix, and copy the certificate to [this file](https://github.com/zhehaowang/bms-node/blob/master/certs/anchor.cert), so that it becomes your root of trust
* Start gateway publisher
  * To follow (tail -f) a file:
<pre>
	python gateway-publisher/bms_publisher.py -f ucla-datahub.log
</pre>
  * Or instead, read all the data points from an existing file.
<pre>
  python gateway-publisher/bms_publisher.py ucla-datahub.log
</pre>
* Option 1: start bms nodes in a customized mini-ndn:
  * Install mini-ndn (this customized fork of mini-ndn contains a bms experiment, and static routes configurable from a file); See the readme [here](https://github.com/named-data/mini-ndn/blob/master/INSTALL.md) for mini-ndn's complete installation instructions.
<pre>
  git clone https://github.com/zhehaowang/mini-ndn
  cd mini-ndn
  sudo ./install.sh -i
</pre>
  * Start bms experiment in mini-ndn
<pre>
  sudo minindn confs/minindn-bms-topology.conf --experiment=bms
</pre>
  Running in mini-ndn takes care of starting multiple bms-nodes with their corresponding configuration files on one physical machine, and configuring the static routes between them. All of this can be done manually on different machines as well.

* Option 2: run individual bms node from command line
<pre>
  python bms_node.py --conf=<configuration file path>
</pre>

* Start consumer: open consumer/index.html in a browser

* Configure routes: interest from bms nodes should be able to get to the gateway publisher, or its child nodes. Interest from the consumer should be able to get to the nodes that publish the expected aggregations.

Contact:
-------
Zhehao Wang: zhehao@remap.ucla.edu
Jiayi Meng: mjycom@hotmail.com