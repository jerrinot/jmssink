package info.jerrinot.o2;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.Rule;
import org.junit.Test;

import javax.jms.*;

import static org.junit.Assert.assertEquals;

public class TestBroker {

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @Test
    public void foo() throws JMSException {
        String queueName = "testQueue";
        String payload = "foo";


        sendMessage("testQueue", "foo");

        QueueSession queueSession = newQueueSession();
        Queue queue = queueSession.createQueue(queueName);
        QueueReceiver receiver = queueSession.createReceiver(queue);

        TextMessage message = (TextMessage) receiver.receive();
        String receivedPayload = message.getText();

        assertEquals(payload, receivedPayload);

    }

    private void sendMessage(String queueName, String payload) throws JMSException {
        QueueSession queueSession = newQueueSession();
        try {
            QueueSender sender = newSender(queueSession, queueName);
            Message message = queueSession.createTextMessage(payload);
            sender.send(message);
        } finally {
            queueSession.close();
        }
    }

    private QueueSender newSender(QueueSession queueSession, String queueName) throws JMSException {
        Queue testQueue = queueSession.createQueue(queueName);
        return queueSession.createSender(testQueue);
    }

    private QueueSession newQueueSession() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = broker.createConnectionFactory();
        QueueConnection queueConnection = connectionFactory.createQueueConnection();
        queueConnection.start();
        return queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    }
}
