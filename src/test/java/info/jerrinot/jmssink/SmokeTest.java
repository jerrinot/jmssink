package info.jerrinot.jmssink;

import com.hazelcast.jet.*;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import info.jerrinot.jmssink.impl.JMSSink;
import info.jerrinot.jmssink.impl.JMSSourceP;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.Rule;
import org.junit.Test;

import javax.jms.*;
import java.util.AbstractMap;
import java.util.Map;

import static com.hazelcast.jet.Sources.mapJournal;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static info.jerrinot.jmssink.api.SinkSupport.asSink;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class SmokeTest extends HazelcastTestSupport {

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @Test
    public void testEnqueing() throws JMSException {
        String sourceMapName = "sourceMap";
        String queueName = "myQueue";

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(mapJournal(sourceMapName, false))
                .map(e -> new AbstractMap.SimpleImmutableEntry(e.getKey(), e.getNewValue()))
                .drainTo(asSink(new JMSSink(broker.getVmURL(), queueName)));

        JetConfig config = new JetConfig();
        config.getHazelcastConfig()
                .getMapEventJournalConfig(sourceMapName)
                .setEnabled(true);
        JetInstance jetInstance = Jet.newJetInstance(config);
        jetInstance.newJob(pipeline);

        IStreamMap<Integer, String> sourceMap = jetInstance.getMap(sourceMapName);
        sourceMap.put(0, "foo");

        assertEntryReceivedEventually(queueName, new AbstractMap.SimpleImmutableEntry<>(0, "foo"));

        jetInstance.shutdown();
    }

    @Test
    public void testQueueAndDequeue() throws JMSException {
        String sourceMapName = "sourceMap";
        String targetMapName = "targetMap";
        String queueName = "myQueue";
        String connectionUrl = broker.getVmURL();

        Pipeline pushingToJMSPipeline = Pipeline.create();
        pushingToJMSPipeline.drawFrom(mapJournal(sourceMapName, false))
                .map(e -> new AbstractMap.SimpleImmutableEntry(e.getKey(), e.getNewValue()))
                .drainTo(asSink(new JMSSink(connectionUrl, queueName)));

        JetConfig config = new JetConfig();
        config.getHazelcastConfig()
                .getMapEventJournalConfig(sourceMapName)
                .setEnabled(true);
        JetInstance jetInstance = Jet.newJetInstance(config);
        Jet.newJetInstance();
        jetInstance.newJob(pushingToJMSPipeline);

        IStreamMap<Integer, String> sourceMap = jetInstance.getMap(sourceMapName);
        sourceMap.put(0, "foo");


        Pipeline pullingFromJMSPipeline = Pipeline.create();
        pullingFromJMSPipeline.drawFrom(Sources.<Map.Entry>fromProcessor("readFromJMS",
                dontParallelize(() -> new JMSSourceP(connectionUrl, queueName))))
                .drainTo(Sinks.map(targetMapName));
        jetInstance.newJob(pullingFromJMSPipeline);


        IStreamMap<Integer, String> targetMap = jetInstance.getMap(targetMapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String s = targetMap.get(0);
                assertEquals(s, "foo");
            }
        });

        jetInstance.shutdown();
    }

    private void assertEntryReceivedEventually(String queueName, Map.Entry entry) throws JMSException {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEntryReceived(queueName, entry);
            }
        });
    }

    private void assertEntryReceived(String queueName, Map.Entry expectedEntry) throws JMSException {
        QueueConnection queueConnection = broker.createConnectionFactory().createQueueConnection();
        queueConnection.start();

        QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = queueSession.createQueue(queueName);
        QueueReceiver receiver = queueSession.createReceiver(queue);

        ObjectMessage received = (ObjectMessage) receiver.receive(SECONDS.toMillis(30));
        Map.Entry entry = (Map.Entry) received.getObject();
        assertEquals(expectedEntry, entry);
    }
}
