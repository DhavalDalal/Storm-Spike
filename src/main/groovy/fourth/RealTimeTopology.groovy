package fourth

import backtype.storm.contrib.jms.bolt.JmsBolt
import backtype.storm.contrib.jms.spout.JmsSpout
import org.springframework.context.support.ClassPathXmlApplicationContext
import second.ShowBolt
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.topology.TopologyBuilder
import third.JmsXmlTupleProducer
import third.SpringJmsProvider
import third.XMLReaderBolt

import javax.jms.Session

def classpathAppContextResource = 'springJms-activemq.xml'
def spring = new ClassPathXmlApplicationContext(classpathAppContextResource)

def reqQueue = new JmsSpout()
reqQueue.jmsProvider = new SpringJmsProvider(spring, 'jmsConnectionFactory', 'wsRequestQueue')
reqQueue.jmsTupleProducer = new JmsJsonTupleProducer()
reqQueue.distributed = true //allow multiple instances
reqQueue.jmsAcknowledgeMode = Session.CLIENT_ACKNOWLEDGE

JmsBolt resQueue = new JmsBolt()
resQueue.jmsProvider = new SpringJmsProvider(spring, 'jmsConnectionFactory', 'wsResponseQueue')
resQueue.jmsAcknowledgeMode = Session.CLIENT_ACKNOWLEDGE
resQueue.jmsMessageProducer = new TupleJsonJmsMessageProducer()

TopologyBuilder builder = new TopologyBuilder()
builder.setSpout('jms', reqQueue, 1)
def myHotelId = 1
builder.setBolt('barCalculator', new BarCalculatorBolt(myHotelId), 2).shuffleGrouping('jms')
builder.setBolt('jsonWriter', new JsonWriterBolt(), 1).shuffleGrouping('barCalculator')
builder.setBolt('publisher', resQueue, 1).shuffleGrouping('jsonWriter')
builder.setBolt('show', new ShowBolt(), 1).shuffleGrouping('jsonWriter')


Config conf = new Config()

if (args != null && args.length >= 1) {
    conf.setNumWorkers(3)
    println('Starting on Remote Cluster...')
    StormSubmitter.submitTopology(args[0], conf, builder.createTopology())
} else {
    conf.setDebug(true)
    println('Starting on Local Cluster...')
    LocalCluster cluster = new LocalCluster()
    cluster.submitTopology('realTimeTopology', conf, builder.createTopology())
}
