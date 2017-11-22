package info.jerrinot.jmssink.impl;

import com.hazelcast.jet.core.AbstractProcessor;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JMSSourceP extends AbstractProcessor {
    private final String connectionUrl;
    private final String queueName;

    private transient QueueSession session;
    private transient QueueReceiver receiver;

    private Object lastItem;

    public JMSSourceP(String connectionUrl, String queueName) {
        this.connectionUrl = connectionUrl;
        this.queueName = queueName;
    }

    @Override
    protected void init(Context context) throws Exception {
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
    public boolean complete() {
        if (lastItem != null) {
            boolean accepted = tryEmit(lastItem);
            if (!accepted) {
                return false;
            }
        }
        do {
            lastItem = tryReceive();
            if (lastItem == null) {
                return false;
            }
        } while (tryEmit(lastItem));

        return false;
    }

    private Object tryReceive() {
        try {
            ObjectMessage objectMessage = (ObjectMessage) receiver.receiveNoWait();
            if (objectMessage == null) {
                return null;
            }
            return objectMessage.getObject();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }
}
