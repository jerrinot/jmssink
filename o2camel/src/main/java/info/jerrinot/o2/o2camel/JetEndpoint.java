package info.jerrinot.o2.o2camel;

import org.apache.camel.Consumer;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public class JetEndpoint<T> extends DefaultEndpoint {
    private final BlockingQueue<T> queue;

    public JetEndpoint(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    public Producer createProducer() throws Exception {
        return new JetProducer(this, queue);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("consumer not implemented yet");
    }

    @Override
    protected String createEndpointUri() {
        return "jet";
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public ExchangePattern getExchangePattern() {
        return super.getExchangePattern();
    }
}
