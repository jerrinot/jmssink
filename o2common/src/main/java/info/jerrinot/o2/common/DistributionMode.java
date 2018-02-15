package info.jerrinot.o2.common;

public enum DistributionMode {
    /**
     * Sink or source will be created on each Jet cluster instance.
     *
     *
     */
    DISTRIBUTED,

    /**
     * There will be a single instance in whole cluster.
     *
     */
    GLOBAL
}
