package com.scylladb.cdc.debezium.connector;

import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class ScyllaSchema implements DatabaseSchema<CollectionId> {
    @Override
    public void close() {
    }

    @Override
    public DataCollectionSchema schemaFor(CollectionId collectionId) {
        // workInProgress

        final Schema keySchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .build();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(valueSchema);

        return new ScyllaCollectionSchema(collectionId, keySchema, valueSchema, envelope);
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }
}
