package info.jerrinot.o2.o2camel;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import javax.jms.ConnectionFactory;
import java.util.concurrent.ArrayBlockingQueue;

public class Foo {
    public static void main(String[] args) throws Exception {

        // START SNIPPET: e1
        CamelContext context = new DefaultCamelContext();
        // END SNIPPET: e1
        // Set up the ActiveMQ JMS Components
        // START SNIPPET: e2
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        // Note we can explicit name the component
        context.addComponent("test-jms", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));

        context.addComponent("jet-local", new JetComponent(new ArrayBlockingQueue(Integer.MAX_VALUE)));
        // END SNIPPET: e2
        // Add some configuration by hand ...
        // START SNIPPET: e3
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("test-jms:queue:test.queue").to("jet-local");
            }
        });
        // END SNIPPET: e3
        // Camel template - a handy class for kicking off exchanges
        // START SNIPPET: e4
        ProducerTemplate template = context.createProducerTemplate();
        // END SNIPPET: e4
        // Now everything is set up - lets start the context
        context.start();
        // Now send some test text to a component - for this case a JMS Queue
        // The text get converted to JMS messages - and sent to the Queue
        // test.queue
        // The file component is listening for messages from the Queue
        // test.queue, consumes
        // them and stores them to disk. The content of each file will be the
        // test we sent here.
        // The listener on the file component gets notified when new files are
        // found ... that's it!
        // START SNIPPET: e5
        for (int i = 0; i < 10; i++) {
            template.sendBody("test-jms:queue:test.queue", "Test Message: " + i);
        }
        // END SNIPPET: e5

        // wait a bit and then stop
        Thread.sleep(1000);
        context.stop();
    }
}
