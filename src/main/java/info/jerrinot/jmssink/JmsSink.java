package info.jerrinot.jmssink;

import com.hazelcast.jet.Sink;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.impl.util.Util;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.Map;

import static com.hazelcast.jet.Sinks.fromProcessor;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.dontParallelize;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeBufferedP;

public class JmsSink {

    public static <E> Sink<E> newQueueSink(String connectionUrl, String queueName) {
        return fromProcessor("jms-queue( + " + queueName + ")", writeJmsP(connectionUrl, queueName));
    }

    public static ProcessorMetaSupplier writeJmsP(String connectionUrl, String queueName) {
        DistributedIntFunction<JmsBuffer> createSenderFb = i -> new JmsBuffer(connectionUrl, queueName);
        DistributedBiConsumer<JmsBuffer, Map.Entry> addToBufferFn = JmsBuffer::addToBuffer;
        DistributedConsumer<JmsBuffer> disposeBufferFn = JmsBuffer::dispose;
        return dontParallelize(
                writeBufferedP(
                        createSenderFb,
                        addToBufferFn,
                        i -> {
                        },
                        disposeBufferFn
                )
        );
    }

    private static final class JmsBuffer {
        private final QueueSession session;
        private final QueueSender sender;

        JmsBuffer(String connectionUrl, String queueName) {
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

        void addToBuffer(Map.Entry entry) {
            Util.uncheckRun(() -> {
                ObjectMessage message = session.createObjectMessage((Serializable) entry);
                sender.send(message);
            });
        }

        void dispose() {
            Util.uncheckRun(() -> {
                sender.close();
                session.close();
            });
        }
    }
}