package com.scylladb.cdc.debezium.connector.tmpclient;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.Generation;
import com.scylladb.cdc.Task;
import com.scylladb.cdc.driver.Reader;
import com.scylladb.cdc.master.GenerationsFetcher;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

// workInProgress
public class TmpMain {
    public static void main(String[] args) {
        for (Generation g : StreamIdsProvider.listAllGenerations()) {
            System.out.println(g.metadata.startTimestamp + " " + g.streamIds.size());
        }
    }
}
