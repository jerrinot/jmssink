package info.jerrinot.jmssink.impl;

import com.hazelcast.jet.core.Processor;
import info.jerrinot.jmssink.api.SourceSupport;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JMSSource<T> extends SourceSupport<T> {
    private final String connectionUrl;
    private final String queueName;

    private transient QueueSession session;
    private transient QueueReceiver receiver;

    public JMSSource(String connectionUrl, String queueName) {
        this.connectionUrl = connectionUrl;
        this.queueName = queueName;
    }

    @Override
    public void init(Processor.Context context) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUrl);
        try {
            QueueConnection queueConnection = connectionFactory.createQueueConnection();
            queueConnection.start();

            session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            receiver = session.createReceiver(queue);

        } catch (JMSException e) {
            //TODO: cleanup
            throw new RuntimeException(e);
        }
    }

    @Override
    public T poll() throws Exception {
        ObjectMessage message = (ObjectMessage) receiver.receiveNoWait();
        return message == null ? null : (T) message.getObject();
    }
}
