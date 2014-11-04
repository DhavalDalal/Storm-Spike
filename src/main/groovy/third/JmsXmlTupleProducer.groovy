package third

import backtype.storm.contrib.jms.JmsTupleProducer
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values

import javax.jms.JMSException
import javax.jms.Message
import javax.jms.TextMessage

class JmsXmlTupleProducer implements JmsTupleProducer {
    @Override
    public Values toTuple(Message msg) throws JMSException {
        if(msg instanceof TextMessage){
            String xmlMessage = ((TextMessage) msg).getText()
            return new Values(xmlMessage)
        }
        return null
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("xml"))
    }
}
