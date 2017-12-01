package info.jerrinot.o2.o2camel;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class JetComponent<T> extends DefaultComponent {
    private final BlockingQueue<T> queue;

    public JetComponent(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    protected Endpoint createEndpoint(String s, String s1, Map<String, Object> map) throws Exception {
        return new JetEndpoint(queue);
    }
}
