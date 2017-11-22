package info.jerrinot.jmssink.api;

public interface LifecycleAware {
    void start() throws Exception;
    void stop() throws Exception;
}
