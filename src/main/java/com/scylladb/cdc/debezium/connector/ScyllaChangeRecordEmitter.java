package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Row;
import io.debezium.data.Envelope;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import org.apache.kafka.connect.data.Struct;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ScyllaChangeRecordEmitter extends AbstractChangeRecordEmitter<ScyllaCollectionSchema> {

    private final Row cdcRow;

    public ScyllaChangeRecordEmitter(Row cdcRow, OffsetContext offsetContext, Clock clock) {
        super(offsetContext, clock);
        this.cdcRow = cdcRow;
    }

    @Override
    protected Envelope.Operation getOperation() {
        return Envelope.Operation.CREATE;
    }

    @Override
    protected void emitReadRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    protected void emitCreateRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        Struct keyStruct = new Struct(scyllaCollectionSchema.keySchema())
                .put("field1", "workInProgress");
        Struct valueStruct = new Struct(scyllaCollectionSchema.valueSchema())
                .put("field1", "workInProgress streamowe: " + cdcRow.toString());

        receiver.changeRecord(scyllaCollectionSchema, getOperation(), keyStruct, valueStruct, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, ScyllaCollectionSchema scyllaCollectionSchema) throws InterruptedException {
        throw new NotImplementedException();
    }
}
