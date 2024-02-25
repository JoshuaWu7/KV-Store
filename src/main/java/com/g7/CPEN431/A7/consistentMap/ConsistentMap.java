package com.g7.CPEN431.A7.consistentMap;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistentMap {
    private final TreeMap<Long , ServerRecord> ring;
    private final int vnodes;
    private final ReadWriteLock lock;
    private final MessageDigest md5;

    public ConsistentMap(int vnodes, String serverPathName) throws IOException, NoSuchAlgorithmException {
        this.ring = new TreeMap<>();
        this.vnodes = vnodes;
        this.lock = new ReentrantReadWriteLock();
        this.md5 = MessageDigest.getInstance("MD5");

        Path path = Paths.get(serverPathName);
        List<String> serverList = Files.readAllLines(path , StandardCharsets.UTF_8);

        for(String server : serverList)
        {
            String[] serverNPort = server.split(":");
            InetAddress addr = InetAddress.getByName(serverNPort[0]);
            int port = serverNPort.length == 2 ? Integer.parseInt(serverNPort[1]): 13788;

            addServer(addr, port);
        }
    }

    public void addServer(InetAddress address, int port)
    {
        lock.writeLock().lock();
        for(int i = 0; i < vnodes; i++)
        {
            ServerRecord vnode = new ServerRecord(address, port, i);
            ring.put(vnode.getHash(), vnode);
        }
        lock.writeLock().unlock();
    }

    public void removeServer(InetAddress address, int port)
    {
        lock.writeLock().lock();
        for(int i = 0; i < vnodes; i++)
        {
            long hashcode = new ServerRecord(address, port, i).getHash();
            ring.remove(hashcode);
        }
        lock.writeLock().unlock();
    }

    public ServerRecord getServer(byte[] key) throws NoServersException {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        long hashcode = getHash(key);

        Map.Entry<Long, ServerRecord> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue();
    }

    public ServerRecord getRandomServer() throws NoServersException
    {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        long hashcode = new Random().nextLong();

        Map.Entry<Long, ServerRecord> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue();
    }

    /**
     * returns whether the server exist in the ring
     * @param addr: the ip address of the server
     * @param port: the port of the server
     * @return whether the server exist in the ring
     */
    public boolean hasServer(InetAddress addr, int port){
        return true;
    }

    private long getHash(byte[] key) {
        md5.reset();

        byte[] dig = md5.digest(key);

        return (
                (long) (dig[7] & 0xFF) << 56 |
                (long) (dig[6] & 0xFF) << 48 |
                (long) (dig[5] & 0xFF) << 40 |
                (long) (dig[4] & 0xFF) << 32 |
                (long) (dig[3] & 0xFF) << 24 |
                (long) (dig[2] & 0xFF) << 16 |
                (long) (dig[1] & 0xFF) << 8 |
                (long) (dig[0] & 0xFF)
                );
    }

public static class NoServersException extends Exception {}

}

