package third

import backtype.storm.contrib.jms.spout.JmsSpout
import org.springframework.context.support.ClassPathXmlApplicationContext
import second.MongoInsertBolt
import second.ShowBolt
import second.TaxBolt
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.topology.TopologyBuilder

import javax.jms.Session

def classpathAppContextResource = 'springJms-activemq.xml'
def spring = new ClassPathXmlApplicationContext(classpathAppContextResource)

def reqQueue = new JmsSpout()
reqQueue.jmsProvider = new SpringJmsProvider(spring, 'jmsConnectionFactory', 'requestQueue')
reqQueue.jmsTupleProducer = new JmsXmlTupleProducer()
reqQueue.distributed = true //allow multiple instances
reqQueue.jmsAcknowledgeMode = Session.CLIENT_ACKNOWLEDGE

TopologyBuilder builder = new TopologyBuilder()
//todo: try changing to parallelism to 2
builder.setSpout('jms', reqQueue, 1)

builder.setBolt('xml', new XMLReaderBolt(), 1).shuffleGrouping('jms')

def taxRates = [
        'AAA' : 10.2,
        'BBB' : 20.3,
        'CCC' : 30.4,
]
builder.setBolt('tax', new TaxBolt(taxRates), 2).shuffleGrouping('xml')

//String mongoUri = args[0]
builder.setBolt('mongoInsert', new MongoInsertBolt('localhost', 27017, 'test', 'dataPoints'), 2).shuffleGrouping('tax')
builder.setBolt('show', new ShowBolt(), 1).shuffleGrouping('tax')

//builder.setBolt('mysql', new RDBMSBolt(), 1).shuffleGrouping('tax')


Config conf = new Config()

if (args != null && args.length >= 1) {
    conf.setNumWorkers(3)
    println('Starting on Remote Cluster...')
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology())
} else {
    conf.setDebug(true)
    println('Starting on Local Cluster...')
    LocalCluster cluster = new LocalCluster()
    cluster.submitTopology('jmsTopology', conf, builder.createTopology())
}
