package com.scylladb.cdc.debezium.connector;

import io.debezium.schema.DataCollectionId;

public class CollectionId implements DataCollectionId {

    private final String keyspaceName;
    private final String tableName;

    public CollectionId() {
        this.keyspaceName = "ks"; // workInProgress
        this.tableName = "t"; // workInProgress
    }

    @Override
    public String identifier() {
        return "workInProgress";
    }

    public String keyspaceName() {
        return keyspaceName;
    }

    public String tableName() {
        return tableName;
    }
}
