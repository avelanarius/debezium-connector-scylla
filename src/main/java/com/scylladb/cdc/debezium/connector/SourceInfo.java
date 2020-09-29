package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.UUIDs;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.util.Collect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.Instant;
import java.util.*;

public class SourceInfo extends BaseSourceInfo {

    public static final String KEYSPACE_NAME = "keyspace_name";
    public static final String TABLE_NAME = "table_name";
    public static final String VNODE_ID = "vnode_id";
    public static final String GENERATION_START = "generation_start";
    public static final String OFFSET = "offset";

    private String keyspaceName;
    private String tableName;
    private Instant timestamp = new Date(0).toInstant();

    private class VNodeAndGeneration {
        public final long vnodeId;
        public final Date generationStart;

        public VNodeAndGeneration(long vnodeId, Date generationStart) {
            this.vnodeId = vnodeId;
            this.generationStart = generationStart;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VNodeAndGeneration that = (VNodeAndGeneration) o;
            return vnodeId == that.vnodeId &&
                    Objects.equals(generationStart, that.generationStart);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vnodeId, generationStart);
        }
    }
    private Map<VNodeAndGeneration, UUID> positionsByVnodeIdGenerationStart = new HashMap<>();

    protected SourceInfo(ScyllaConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.keyspaceName = "ks"; // workInProgress
        this.tableName = "t"; // workInProgress
    }

    public Map<String, String> partition(long vnodeId, Date generationStart) {
        return Collect.hashMapOf(KEYSPACE_NAME, keyspaceName,
                TABLE_NAME, tableName, VNODE_ID, Long.toString(vnodeId),
                GENERATION_START, Long.toString(generationStart.getTime()));
    }

    public Map<String, String> offset(long vnodeId, Date generationStart) {
        UUID position = lastOffsetUUID(vnodeId, generationStart);
        return Collect.hashMapOf(OFFSET, position.toString());
    }

    public UUID lastOffsetUUID(long vnodeId, Date generationStart) {
        VNodeAndGeneration entry = new VNodeAndGeneration(vnodeId, generationStart);
        UUID position = positionsByVnodeIdGenerationStart.get(entry);
        if (position == null) {
            position = UUIDs.startOf(0);
        }
        return position;
    }

    public void loadLastOffsetUUID(long vnodeId, Date generationStart, UUID time) {
        positionsByVnodeIdGenerationStart.put(new VNodeAndGeneration(vnodeId, generationStart), time);
        timestamp = new Date(UUIDs.unixTimestamp(time)).toInstant();
    }

    public void dataChangeEvent(long vnodeId, Date generationStart, UUID time) {
        loadLastOffsetUUID(vnodeId, generationStart, time);
    }

    public String keyspaceName() {
        return keyspaceName;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        return "TODO - database()";
    }
}
