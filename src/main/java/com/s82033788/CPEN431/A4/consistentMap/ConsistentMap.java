package com.s82033788.CPEN431.A4.consistentMap;

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
    private final TreeMap<Integer, ServerRecord> ring;
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
            String[] serverNPort = server.split(":");
            InetAddress addr = InetAddress.getByName(serverNPort[0]);
            int port = serverNPort.length == 2 ? Integer.parseInt(serverNPort[1]): 13788;

            addServer(new ServerRecord(addr, port));
        }
    }

    public void addServer(ServerRecord address)
    {
        lock.writeLock().lock();
        for(int i = 0; i < vnodes; i++)
        {
            int hashcode = Objects.hash(address, i);
            ring.put(hashcode, address);
        }
        lock.writeLock().unlock();
    }

    public void removeServer(ServerRecord address)
    {
        lock.writeLock().lock();
        for(int i = 0; i < vnodes; i++)
        {
            int hashcode = Objects.hash(address, i);
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

        int hashcode = Arrays.hashCode(key);

        Map.Entry<Integer, ServerRecord> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue();
    }

public class NoServersException extends Exception {}

}

