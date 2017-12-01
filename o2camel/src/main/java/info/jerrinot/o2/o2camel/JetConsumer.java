package info.jerrinot.o2.o2camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

public class JetConsumer extends DefaultConsumer {

    public JetConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }
}
