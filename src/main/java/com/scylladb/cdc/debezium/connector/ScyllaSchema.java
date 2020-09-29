package com.scylladb.cdc.debezium.connector;

import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class ScyllaSchema implements DatabaseSchema<CollectionId> {
    private final Schema sourceSchema;

    public ScyllaSchema(Schema sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

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
                .field(Envelope.FieldName.SOURCE, sourceSchema)
                .field(Envelope.FieldName.AFTER, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .field(Envelope.FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                .build();

        final Envelope envelope = Envelope.fromSchema(valueSchema);

        return new ScyllaCollectionSchema(collectionId, keySchema, valueSchema, envelope);
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }
}
