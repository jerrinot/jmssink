package info.jerrinot.o2.o2camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;

import java.util.concurrent.BlockingQueue;

public final class JetProducer<T> extends DefaultProducer {
    private final BlockingQueue<T> queue;

    public JetProducer(Endpoint endpoint, BlockingQueue<T> queue) {
        super(endpoint);
        this.queue = queue;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Object body = exchange.getIn().getBody();
        System.out.println("Received: '" + body + "', type '" + body.getClass().getName() + "'");
        queue.put((T) body);
    }
}
