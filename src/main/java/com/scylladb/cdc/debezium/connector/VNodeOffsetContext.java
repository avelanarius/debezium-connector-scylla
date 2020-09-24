package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class VNodeOffsetContext implements OffsetContext {

    private final ScyllaOffsetContext offsetContext;
    private final long vnodeId;
    private final SourceInfo sourceInfo;

    public VNodeOffsetContext(ScyllaOffsetContext offsetContext, long vnodeId, SourceInfo sourceInfo) {
        this.offsetContext = offsetContext;
        this.vnodeId = vnodeId;
        this.sourceInfo = sourceInfo;
    }

    @Override
    public Map<String, ?> getPartition() {
        return sourceInfo.partition(vnodeId);
    }

    @Override
    public Map<String, ?> getOffset() {
        return sourceInfo.offset(vnodeId);
    }

    public UUID lastOffsetUUID() {
        return sourceInfo.lastOffsetUUID(vnodeId);
    }

    public void dataChangeEvent(UUID time) {
        sourceInfo.dataChangeEvent(vnodeId, time);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return offsetContext.getSourceInfoSchema();
    }

    @Override
    public Struct getSourceInfo() {
        return offsetContext.getSourceInfo();
    }

    @Override
    public boolean isSnapshotRunning() {
        return offsetContext.isSnapshotRunning();
    }

    @Override
    public void markLastSnapshotRecord() {
        offsetContext.markLastSnapshotRecord();
    }

    @Override
    public void preSnapshotStart() {
        offsetContext.preSnapshotStart();
    }

    @Override
    public void preSnapshotCompletion() {
        offsetContext.preSnapshotCompletion();
    }

    @Override
    public void postSnapshotCompletion() {
        offsetContext.postSnapshotCompletion();
    }

    @Override
    public void event(DataCollectionId dataCollectionId, Instant instant) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionContext getTransactionContext() {
        return offsetContext.getTransactionContext();
    }
}
