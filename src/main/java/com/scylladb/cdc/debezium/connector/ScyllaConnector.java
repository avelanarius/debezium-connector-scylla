package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.Generation;
import com.scylladb.cdc.debezium.connector.tmpclient.StreamIdsProvider;
import io.debezium.config.Configuration;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class ScyllaConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;
    private Configuration config;
    private Thread workInProgressRefreshThread;

    public ScyllaConnector() {
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        final Configuration config = Configuration.from(props);
        this.config = config;

        this.workInProgressRefreshThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    ScyllaConnector.this.context.requestTaskReconfiguration();
                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        this.workInProgressRefreshThread.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ScyllaConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // workInProgress
        // Preliminary "algorithm"

        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.2").withPort(9042).build();
        Session session = cluster.connect();

        final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
        SourceInfo sourceInfo = new SourceInfo(connectorConfig);

        // 1. Get all generations
        Collection<Generation> generations = StreamIdsProvider.listAllGenerations();

        Date safetyNow = Date.from(Instant.now().minusSeconds(30));

        // 2. Find the generation we should be currently working on
        Generation chosenGeneration = null;
        for (Generation generation : generations) { // Important to iterate from oldest to newest
            Map<Long, List<String>> splitByVNodes = StreamIdsProvider.splitStreamIdsByVNodesMap(
                    generation.streamIds.stream().map(Bytes::toHexString).collect(Collectors.toList()));

            if (generation.metadata.startTimestamp.after(safetyNow)) {
                break;
            }

            boolean workToDo = false;

            // Find out if there is still work to do on this generation
            for (Map.Entry<Long, List<String>> entry : splitByVNodes.entrySet()) {
                Map<String, Object> readOffset = context().offsetStorageReader().offset(sourceInfo.partition(entry.getKey()));
                UUID lastOffsetUUID = UUIDs.startOf(0);
                if (readOffset != null) {
                    lastOffsetUUID = UUID.fromString((String) readOffset.get(SourceInfo.OFFSET));
                }

                // workInProgress
                for (String streamId : entry.getValue()) {
                    String queryString = "SELECT * FROM ks.t_scylla_cdc_log WHERE \"cdc$stream_id\" = " + streamId
                            + " AND \"cdc$time\" > " + lastOffsetUUID.toString();
                    ResultSet rs = session.execute(queryString);
                    if (!rs.isExhausted()) {
                        // There is still work to do on this generation
                        workToDo = true;
                    }
                }
            }

            if (workToDo) {
                chosenGeneration = generation;
                break;
            }
        }

        if (chosenGeneration == null) {
            // In all generations work is done, so choose the latest generation (minding the safety window)
            List<Generation> filteredGenerations = generations.stream().filter(g -> g.metadata.startTimestamp.before(safetyNow)).collect(Collectors.toList());
            if (!filteredGenerations.isEmpty()) {
                chosenGeneration = filteredGenerations.get(filteredGenerations.size() - 1);
            }
        }

        session.close();
        cluster.close();

        List<Map<String, String>> tasks = new ArrayList<>();

        if (chosenGeneration != null) {
            Collection<List<String>> streamIdsList = StreamIdsProvider.splitStreamIdsByVNodes(chosenGeneration.streamIds.stream().map(Bytes::toHexString).collect(Collectors.toList()));
            Collection<List<String>> chunks = StreamIdsProvider.reduceCount(streamIdsList, 5);

            for (List<String> chunk : chunks) {
                Map<String, String> taskConfig = config.edit()
                        .with(ScyllaConnectorConfig.STREAM_IDS, String.join(",", chunk))
                        .build().asMap();
                tasks.add(taskConfig);
            }
        }

        System.out.println("Will queue up: " + tasks.size());
        return tasks;
    }

    @Override
    public void stop() {
        this.workInProgressRefreshThread.stop();
    }

    @Override
    public String version() {
        return "workInProgress";
    }

    @Override
    public ConfigDef config() {
        return ScyllaConnectorConfig.configDef();
    }
}
