package com.scylladb.cdc.debezium.connector.tmpclient;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.utils.Bytes;
import com.scylladb.cdc.Generation;
import com.scylladb.cdc.driver.Reader;
import com.scylladb.cdc.master.GenerationsFetcher;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class StreamIdsProvider {
    // workInProgress
    private static final class DecoratedKey implements Comparable<DecoratedKey> {
        public final ByteBuffer key;
        public final Token token;

        public DecoratedKey(ByteBuffer k, Token t) {
            key = k;
            token = t;
        }

        @Override
        public int compareTo(DecoratedKey o) {
            return token.compareTo(o.token);
        }
    }

    public static Collection<List<String>> reduceCount(Collection<List<String>> list, int desiredCount) {
        if (list.size() <= desiredCount) {
            return list;
        }
        List<List<String>> result = new ArrayList<>(desiredCount);
        for (int i = 0; i < desiredCount; i++) {
            result.add(new ArrayList<>());
        }
        int rr = 0;
        // FIXME - temporary round-robin implementation
        for (List<String> element : list) {
            result.get(rr).addAll(element);
            rr = (rr + 1) % desiredCount;
        }
        return result;
    }

    private static String helper(ByteBuffer key, Token t) {
        long lowerDword = key.getLong(key.position() + 8);
        lowerDword = lowerDword & 0x3FFFFF0;
        return Bytes.toHexString(key) + " assosciated with " + t + " =? " + (lowerDword >> 4);
    }

    public static long vnodeNumberForStreamId(String streamId) {
        ByteBuffer streamIdBytes = Bytes.fromHexString(streamId);
        long lowerDword = streamIdBytes.getLong(streamIdBytes.position() + 8);
        long vnodeId = (lowerDword & 0x3FFFFF0) >> 4;
        return vnodeId;
    }

    public static Map<Long, List<String>> splitStreamIdsByVNodesMap(Collection<String> streamIds) {
        Map<Long, List<String>> vnodeToStreamIds = new HashMap<>();
        for (String streamId : streamIds) {
            long vnodeId = vnodeNumberForStreamId(streamId);
            vnodeToStreamIds.computeIfAbsent(vnodeId, q -> new ArrayList<>()).add(streamId);
        }
        return vnodeToStreamIds;
    }

    public static Collection<List<String>> splitStreamIdsByVNodes(Collection<String> streamIds) {
        return splitStreamIdsByVNodesMap(streamIds).values();
    }

    public static Collection<List<String>> splitStreams() {
        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.2").build();
        Session session = cluster.connect();
        Reader<Date> tReader = Reader.createGenerationsTimestampsReader(session);
        Reader<Set<ByteBuffer>> gsReader = Reader.createGenerationStreamsReader(session);
        GenerationsFetcher fetcher = new GenerationsFetcher(tReader, gsReader);

        Generation g = null;
        try {
            g = fetcher.fetchNext(new Date(0), false).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        assert g != null;
        Set<ByteBuffer> streamIds = g.streamIds;

        session.close();
        cluster.close();

        Collection<List<String>> streamIdsList = splitStreamIdsByVNodes(streamIds.stream().map(Bytes::toHexString).collect(Collectors.toList()));

        return reduceCount(streamIdsList, 5);
    }

    public static Collection<Generation> listAllGenerations() {
        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.2").build();
        Session session = cluster.connect();
        Reader<Date> tReader = Reader.createGenerationsTimestampsReader(session);
        Reader<Set<ByteBuffer>> gsReader = Reader.createGenerationStreamsReader(session);
        GenerationsFetcher fetcher = new GenerationsFetcher(tReader, gsReader);

        List<Generation> generations = new ArrayList<>();

        Date currentDate = new Date(0);
        Generation g = null;
        try {
            do {
                g = fetcher.fetchNext(currentDate, true).get();
                if (g != null) {
                    currentDate = g.metadata.startTimestamp;
                    generations.add(g);
                }
            } while (g != null);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        session.close();
        cluster.close();

        return generations;
    }

}
