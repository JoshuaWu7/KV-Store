package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.cache.RequestCacheKey;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Timer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hello world!
 *
 */
public class KVServer
{
    /* Default port, overwritten by cmd line */
    static int PORT = 13788;
    final static int N_THREADS = 4;
    static final int PACKET_MAX = 16384;
    final static long CACHE_EXPIRY = 1;
    final static int QUEUE_MAX = 8;
    final static int MEMORY_SAFETY = 2_097_152;
    final static int AVG_KEY_SZ = 32;
    final static int MAP_ENTRIES = 146_800;
    final static int AVG_VAL_SZ = 500;
    final static String SERVER_LIST = "servers.txt";
    final static int VNODE_COUNT = 4;
    final static int GOSSIP_INTERVAL = 500;
    final static int GOSSIP_WAIT_INIT = 8000;
    public static ServerRecord self;
    public static ServerRecord selfLoopback;


    public static void main( String[] args )
    {
        try
        {

           PORT = Integer.parseInt(args[0]);
           self = new ServerRecord(InetAddress.getLocalHost(), PORT, 0);
           selfLoopback = new ServerRecord(InetAddress.getLoopbackAddress(), PORT, 0);



            DatagramSocket server = new DatagramSocket(PORT);
            /* Eliminated in single thread */
//            ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);

            ConcurrentMap<KeyWrapper, ValueWrapper> map
                    = ChronicleMap
                    .of(KeyWrapper.class, ValueWrapper.class)
                    .name("KVStore")
                    .averageKeySize(AVG_KEY_SZ)
                    .entries(MAP_ENTRIES)
                    .averageValueSize(AVG_VAL_SZ)
                    .create();

            /*
            * Explanation of the mapLock.
            *
            * Read lock can be accessed by multiple parties at once. This is used to
            * control access to the map (which is thread safe in itself, and code is also written to be thread safe),
            * so we can support concurrent accesses and mutations. However, a read lock cannot be obtained at the same
            * time a write lock is obtained by some other thread. Used by handleget, handleDelete, handlePut
            *
            * Write lock can only be accessed by one party (so it guarantees exclusive access to the map, no other
            * threads will write). This is used to clear the map and send the response atomically to ensure correctness.
            * Used only by handleWipeout
            *
            * */
            AtomicInteger bytesUsed = new AtomicInteger(0);
            ReadWriteLock mapLock = new ReentrantReadWriteLock();

            @SuppressWarnings("UnstableApiUsage") Cache<RequestCacheKey, DatagramPacket> requestCache = CacheBuilder.newBuilder()
                    .expireAfterWrite(CACHE_EXPIRY, TimeUnit.SECONDS)
                    //.maximumSize(131072)
                    .build();

            /* Setup pool of byte arrays - single thread implementation only has 1 */
            /* A simpler approach to keeping track of byte arrays*/
            ConcurrentLinkedQueue<byte[]> bytePool = new ConcurrentLinkedQueue<>();
            for(int i = 0; i < N_THREADS; i++) {
                bytePool.add(new byte[PACKET_MAX]);
            }

            /* Outbound Queue and Thread - eliminated in single thread implementation */
            ConcurrentLinkedQueue<DatagramPacket> outbound = new ConcurrentLinkedQueue<>();
//            executor.execute(() -> {
//                while (true) {
//                    if(!outbound.isEmpty()) {
//                        try {
//                            server.send(outbound.poll());
//                        } catch (IOException e) {
//                            System.err.println("Failure to send packets");
//                            throw new RuntimeException(e);
//                        }
//                    } else {
//                        Thread.yield();
//                    }
//                }
//            });

            /* Set up the list of servers */
            ConsistentMap serverRing = new ConsistentMap(VNODE_COUNT, SERVER_LIST);

            /* Set up obituary list */
            ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths = new ConcurrentLinkedQueue();

            /* set up the timer */
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new DeathRegistrar(pendingRecordDeaths, serverRing), GOSSIP_WAIT_INIT, GOSSIP_INTERVAL);

            while(true){

                Runtime r = Runtime.getRuntime();
                long remainingMemory  = r.maxMemory() - (r.totalMemory() - r.freeMemory());
                boolean isOverloaded = remainingMemory < MEMORY_SAFETY;

                while(bytePool.isEmpty()) Thread.yield();
                byte [] iBuf = bytePool.poll();

                DatagramPacket iPacket = new DatagramPacket(iBuf, iBuf.length);
                server.receive(iPacket);

                /* Run it directly instead of via executor service. */
                new KVServerTaskHandler(
                        iPacket,
                        requestCache,
                        map,
                        mapLock,
                        bytesUsed,
                        bytePool,
                        isOverloaded,
                        outbound,
                        serverRing,
                        pendingRecordDeaths).run();

                /* Executed here in single thread impl. */
                while(!outbound.isEmpty())
                {
                    server.send(outbound.poll());
                }
            }

        } catch (SocketException e) {
            //System.err.println("Server socket setup exception");
            throw new RuntimeException(e);
        } catch (IOException e) {
            //System.err.println("Server IO exception.");
            throw new RuntimeException(e);
        } catch (Exception e) {
//            System.err.println("Bytepool exception");
            throw new RuntimeException(e);
        }

    }
}
