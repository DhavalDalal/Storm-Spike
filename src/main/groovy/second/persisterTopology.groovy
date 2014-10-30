package second

import second.rdbms.RDBMSBolt
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.Utils

TopologyBuilder builder = new TopologyBuilder()

String xmlDataFileUri = args[0]
builder.setSpout('xml', new XMLReaderSpout(xmlDataFileUri), 2)

def taxRates = [
        'AAA' : 10.2,
        'BBB' : 20.3,
        'CCC' : 30.4
]
builder.setBolt('tax', new TaxBolt(taxRates), 2).shuffleGrouping('xml')

//String mongoUri = args[1]
builder.setBolt('mongoInsert', new MongoInsertBolt('localhost', 27017, 'test', 'dataPoints'), 2).shuffleGrouping('tax')
builder.setBolt('show', new ShowBolt(), 1).shuffleGrouping('tax')

builder.setBolt('mysql', new RDBMSBolt(), 1).shuffleGrouping('tax')


Config conf = new Config()

if (args != null && args.length > 2) {
    conf.setNumWorkers(3)
    println('Starting on Remote Cluster...')
    StormSubmitter.submitTopology(args[2], conf, builder.createTopology())
} else {
    conf.setDebug(true)
    println('Starting on Local Cluster...')
    LocalCluster cluster = new LocalCluster()

    cluster.submitTopology('aggregationTopology', conf, builder.createTopology())
    Utils.sleep(10000)
    cluster.killTopology('aggregationTopology')
}
