package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

public class ScyllaChangeEventSourceFactory implements ChangeEventSourceFactory {

    private ScyllaConnectorConfig configuration;
    private ScyllaTaskContext taskContext;
    private final EventDispatcher<CollectionId> dispatcher;
    private final Clock clock;

    public ScyllaChangeEventSourceFactory(ScyllaConnectorConfig configuration, ScyllaTaskContext context, EventDispatcher<CollectionId> dispatcher, Clock clock) {
        this.configuration = configuration;
        this.taskContext = context;
        this.dispatcher = dispatcher;
        this.clock = clock;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new ScyllaSnapshotChangeEventSource(configuration, (ScyllaOffsetContext) offsetContext, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new ScyllaStreamingChangeEventSource(configuration, taskContext, (ScyllaOffsetContext) offsetContext, dispatcher, clock);
    }
}
