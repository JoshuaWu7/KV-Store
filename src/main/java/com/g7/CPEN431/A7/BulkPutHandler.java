package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.cache.RequestCacheKey;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.google.common.cache.Cache;

import java.net.DatagramPacket;
import java.util.Timer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

public class BulkPutHandler extends KVServerTaskHandler {
    public BulkPutHandler(DatagramPacket iPacket, Cache<RequestCacheKey, DatagramPacket> requestCache, ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock mapLock, AtomicInteger bytesUsed, BlockingQueue<byte[]> bytePool, boolean isOverloaded, ConcurrentLinkedQueue<DatagramPacket> outbound, ConsistentMap serverRing, ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths, ExecutorService threadPool, AtomicLong lastReqTime, Semaphore keyUpdateRequested, Timer timer) {
        super(iPacket, requestCache, map, mapLock, bytesUsed, bytePool, isOverloaded, outbound, serverRing, pendingRecordDeaths, threadPool, lastReqTime, keyUpdateRequested, timer);
    }

    @Override
    public void run() {
        Thread.currentThread().setPriority(Thread.NORM_PRIORITY + 1);
        super.run();
    }
}
