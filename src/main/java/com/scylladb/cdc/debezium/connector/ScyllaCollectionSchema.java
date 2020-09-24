package com.scylladb.cdc.debezium.connector;

import io.debezium.data.Envelope;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import org.apache.kafka.connect.data.Schema;

public class ScyllaCollectionSchema implements DataCollectionSchema {
    private final CollectionId id;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final Envelope envelopeSchema;

    public ScyllaCollectionSchema(CollectionId id, Schema keySchema, Schema valueSchema, Envelope envelopeSchema) {
        this.id = id;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.envelopeSchema = envelopeSchema;
    }

    @Override
    public DataCollectionId id() {
        return id;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    public Schema valueSchema() { return valueSchema; }

    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }
}
