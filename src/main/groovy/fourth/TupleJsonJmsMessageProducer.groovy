package fourth

import backtype.storm.contrib.jms.JmsMessageProducer

import javax.jms.Destination
import javax.jms.JMSException
import javax.jms.Message
import javax.jms.Session
import backtype.storm.tuple.Tuple

class TupleJsonJmsMessageProducer implements JmsMessageProducer {
    @Override
    Message toMessage(Session session, Tuple input) throws JMSException {
        String jsonString = input.getString(0)
        session.createTextMessage(jsonString)
    }
}
