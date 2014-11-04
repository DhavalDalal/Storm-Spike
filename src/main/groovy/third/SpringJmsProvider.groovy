package third

import backtype.storm.contrib.jms.JmsProvider

import javax.jms.ConnectionFactory
import javax.jms.Destination

class SpringJmsProvider implements JmsProvider {

    private def transient spring
    private ConnectionFactory connectionFactory
    private Destination destination

    SpringJmsProvider(spring, connectionFactory, destination) {
        this.spring = spring
        this.connectionFactory = spring.getBean(connectionFactory) as ConnectionFactory
        this.destination = spring.getBean(destination) as Destination
    }

    @Override
    ConnectionFactory connectionFactory() throws Exception {
        connectionFactory
    }

    @Override
    Destination destination() throws Exception {
        destination
    }
}
