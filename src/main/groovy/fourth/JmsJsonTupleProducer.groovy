package fourth

import backtype.storm.contrib.jms.JmsTupleProducer
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import groovy.json.JsonSlurper

import javax.jms.JMSException
import javax.jms.Message
import javax.jms.TextMessage

class JmsJsonTupleProducer implements JmsTupleProducer {

    @Override
    public Values toTuple(Message msg) throws JMSException {
        if(msg instanceof TextMessage){
            String jsonMessage = ((TextMessage) msg).text
            println "Got Message $jsonMessage"
            def json = new JsonSlurper().parseText(jsonMessage)
            return new Values(json['id'], json['rate'])
        }
        return null
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields('id', 'rate'))
    }
}

