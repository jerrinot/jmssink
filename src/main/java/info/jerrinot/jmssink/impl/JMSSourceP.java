package info.jerrinot.jmssink.impl;

import com.hazelcast.jet.core.AbstractProcessor;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JMSSourceP extends AbstractProcessor {
    private final String connectionUrl;
    private final String queueName;

    private transient QueueSession session;
    private transient QueueReceiver receiver;

    private Object pendingItem;

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
        if (!tryToSendPendingItem()) {
            return false;
        }
        do {
            pendingItem = tryReceive();
            if (pendingItem == null) {
                return false;
            }
        } while (tryEmit(pendingItem));

        return false;
    }

    private boolean tryToSendPendingItem() {
        if (pendingItem == null) {
            return true;
        }
        return tryEmit(pendingItem);
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
