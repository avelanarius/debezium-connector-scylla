package com.scylladb.cdc.debezium.connector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collections;

public class ScyllaSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource {

    private final ScyllaConnectorConfig connectorConfig;
    private final ScyllaOffsetContext previousOffset;
    private final SnapshotProgressListener snapshotProgressListener;

    public ScyllaSnapshotChangeEventSource(ScyllaConnectorConfig connectorConfig, ScyllaOffsetContext previousOffset, SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.previousOffset = previousOffset;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    protected SnapshotResult doExecute(ChangeEventSourceContext changeEventSourceContext, SnapshotContext snapshotContext, SnapshottingTask snapshottingTask) throws Exception {
        snapshotProgressListener.snapshotCompleted();
        return SnapshotResult.completed(snapshotContext.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext offsetContext) {
        return new SnapshottingTask(false, false);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {

    }
}
