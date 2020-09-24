package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.UUIDs;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.util.Collect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SourceInfo extends BaseSourceInfo {

    public static final String KEYSPACE_NAME = "keyspace_name";
    public static final String TABLE_NAME = "table_name";
    public static final String VNODE_ID = "vnode_id";
    public static final String OFFSET = "offset";

    private String keyspaceName;
    private String tableName;
    private Map<Long, UUID> positionsByVnodeId = new HashMap<>();

    protected SourceInfo(ScyllaConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.keyspaceName = "ks"; // workInProgress
        this.tableName = "t"; // workInProgress
    }

    public Map<String, String> partition(long vnodeId) {
        return Collect.hashMapOf(KEYSPACE_NAME, keyspaceName,
                TABLE_NAME, tableName, VNODE_ID, Long.toString(vnodeId));
    }

    public Map<String, String> offset(long vnodeId) {
        UUID position = lastOffsetUUID(vnodeId);
        return Collect.hashMapOf(OFFSET, position.toString());
    }

    public UUID lastOffsetUUID(long vnodeId) {
        UUID position = positionsByVnodeId.get(vnodeId);
        if (position == null) {
            position = UUIDs.startOf(0);
        }
        return position;
    }

    public void loadLastOffsetUUID(long vnodeId, UUID time) {
        positionsByVnodeId.put(vnodeId, time);
    }

    public void dataChangeEvent(long vnodeId, UUID time) {
        positionsByVnodeId.put(vnodeId, time);
    }

    @Override
    protected Instant timestamp() {
        throw new NotImplementedException();
    }

    @Override
    protected String database() {
        return "TODO - database()";
    }
}
