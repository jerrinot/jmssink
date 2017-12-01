package info.jerrinot.o2.o2camel;

import com.hazelcast.jet.*;
import com.hazelcast.jet.stream.IStreamMap;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.apache.camel.component.jms.JmsComponent;
import org.junit.Rule;
import org.junit.Test;

import javax.jms.*;
import java.util.UUID;

import static com.hazelcast.jet.Util.entry;


public class SmokeTestJMS {

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @Test
    public void testSource() throws JMSException, InterruptedException {
        String brokerUrl = broker.getVmURL();
        Source<String> camelSource = new CamelSource<String>(
                Pair.of(
                        "test-jms")
                    .and(
                        () -> JmsComponent.jmsComponentAutoAcknowledge(new ActiveMQConnectionFactory(brokerUrl))
                    ),
                "test-jms:queue:test.queue")
                .asSource();

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(camelSource)
                .map(e -> entry(UUID.randomUUID().toString(), e))
                .drainTo(Sinks.map("targetMap"));

        JetInstance jetInstance = Jet.newJetInstance();
//        Jet.newJetInstance();
        jetInstance.newJob(pipeline);

        for (int i = 0; i < 10; i++) {
            sendMessage("test.queue", "value no. " + i);
        }
        // END SNIPPET: e5

        IStreamMap<String, String> targetMap = jetInstance.getMap("targetMap");
        for (;;) {
            System.out.println("Map size: " + targetMap.size());
            Thread.sleep(500);
        }


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
