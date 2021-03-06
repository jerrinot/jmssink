package info.jerrinot.o2.impl;

import com.hazelcast.jet.impl.pipeline.JetEvent;
import info.jerrinot.o2.common.SinkSupport;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;

public final class JMSSink implements SinkSupport<Object> {
    private final String connectionUrl;
    private final String queueName;

    private transient QueueSession session;
    private transient QueueSender sender;

    public JMSSink(String connectionUrl, String queueName) {
        this.connectionUrl = connectionUrl;
        this.queueName = queueName;
    }

    @Override
    public void doInvoke(Object o) throws JMSException {
        ObjectMessage message = session.createObjectMessage((Serializable) o);
        sender.send(message);
    }

    @Override
    public void start() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUrl);
        try {
            QueueConnection queueConnection = connectionFactory.createQueueConnection();
            queueConnection.start();

            session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            sender = session.createSender(queue);

        } catch (JMSException e) {
            //TODO: cleanup
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() throws JMSException {
        try {
            sender.close();
        } finally {
            session.close();
        }
    }

    @Override
    public String toString() {
        return "JMSSink{" +
                "connectionUrl='" + connectionUrl + '\'' +
                ", queueName='" + queueName + '\'' +
                '}';
    }
}
