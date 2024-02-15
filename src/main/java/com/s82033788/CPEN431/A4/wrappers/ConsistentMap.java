package com.s82033788.CPEN431.A4.wrappers;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConsistentMap {
    private final TreeMap<Integer, InetAddress> ring;
    private final int vnodes;
    private final ReadWriteLock lock;

    public ConsistentMap(int vnodes, String serverPathName) throws IOException {
        this.ring = new TreeMap<>();
        this.vnodes = vnodes;
        this.lock = new ReentrantReadWriteLock();

        Path path = Paths.get(serverPathName);
        List<String> serverList = Files.readAllLines(path , StandardCharsets.UTF_8);

        for(String server : serverList)
        {
            InetAddress addr = InetAddress.getByName(server);
            addServer(addr);
        }
    }

    public void addServer(InetAddress address)
    {
        lock.writeLock().lock();
        for(int i = 0; i < vnodes; i++)
        {
            int hashcode = Objects.hash(address, i);
            ring.put(hashcode, address);
        }
        lock.writeLock().unlock();
    }

    public void removeServer(InetAddress address)
    {
        lock.writeLock().lock();
        for(int i = 0; i < vnodes; i++)
        {
            int hashcode = Objects.hash(address, i);
            ring.remove(hashcode);
        }
        lock.writeLock().unlock();
    }

    public InetAddress getServer(byte[] key) throws NoServersException {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        int hashcode = Arrays.hashCode(key);

        Map.Entry<Integer, InetAddress> server = ring.floorEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue();
    }

class NoServersException extends Exception {}

}

