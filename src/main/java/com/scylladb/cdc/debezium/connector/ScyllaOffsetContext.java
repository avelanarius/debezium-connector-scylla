package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class ScyllaOffsetContext implements OffsetContext {

    private final SourceInfo sourceInfo;
    private final TransactionContext transactionContext;

    public ScyllaOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext) {
        this.sourceInfo = sourceInfo;
        this.transactionContext = transactionContext;
    }

    public VNodeOffsetContext vnodeOffsetContext(long vnodeId, Date generationStart) {
        return new VNodeOffsetContext(this, vnodeId, generationStart, sourceInfo);
    }

    @Override
    public Map<String, ?> getPartition() {
        // See VNodeOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ?> getOffset() {
        // See VNodeOffsetContext
        throw new UnsupportedOperationException();
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markLastSnapshotRecord() {

    }

    @Override
    public void preSnapshotStart() {

    }

    @Override
    public void preSnapshotCompletion() {

    }

    @Override
    public void postSnapshotCompletion() {

    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {

    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }
}
