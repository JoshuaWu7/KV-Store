package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.s82033788.CPEN431.A4.proto.RequestCacheKey;
import com.s82033788.CPEN431.A4.proto.RequestCacheValue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class KVServer
{
    final static int PORT = 13788;
    final static int N_THREADS = 128; //TODO tune by profiler
    static final int PACKET_MAX = 16384;
    final static long CACHE_SZ = 64;//TODO tune by profiler
    final static long CACHE_EXPIRY = 5;
    public static void main( String[] args )
    {

        try
        {
            DatagramSocket server = new DatagramSocket(PORT);
            ExecutorService executor = Executors.newFixedThreadPool(N_THREADS); //TODO tune profiler
            byte[] iBuf = new byte[PACKET_MAX];
            Cache<RequestCacheKey, RequestCacheValue> requestCache = CacheBuilder.newBuilder()
                    .maximumSize(CACHE_SZ)
                    .expireAfterWrite(CACHE_EXPIRY, TimeUnit.SECONDS)
                    .build(/* TODO add cacheloader*/);



            while(true){
                server.receive(new DatagramPacket(iBuf, iBuf.length));

                executor.execute(() -> {

                }/* TODO insert runnable task*/);




            }

        } catch (SocketException e) {
            System.err.println("Server socket setup exception");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("Server IO exception.");
            throw new RuntimeException(e);
        }

    }
}
