package com.g7.CPEN431.A7.consistentMap;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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

import static com.g7.CPEN431.A7.KVServer.self;
import static com.g7.CPEN431.A7.KVServer.selfLoopback;

public class ConsistentMap {
    private final TreeMap<Integer, VNode> ring;
    private final int VNodes;
    private final ReadWriteLock lock;
    private int current = 0;

    public ConsistentMap(int vNodes, String serverPathName) throws IOException  {
        this.ring = new TreeMap<>();
        this.VNodes = vNodes;
        this.lock = new ReentrantReadWriteLock();

        Path path = Paths.get(serverPathName);
        List<String> serverList = Files.readAllLines(path , StandardCharsets.UTF_8);

        for(String server : serverList)
        {
            String[] serverNPort = server.split(":");
            InetAddress addr = InetAddress.getByName(serverNPort[0]);
            int port = serverNPort.length == 2 ? Integer.parseInt(serverNPort[1]): 13788;

            ServerRecord serverRecord = addServerPrivate(addr, port);

            /* only activated during initialization, initializes current ptr */
            if(serverRecord.equals(self) || serverRecord.equals(selfLoopback))
            {
                this.current = new VNode(self, 0).getHash() + 1;
            }
        }
    }

    public void addServer(InetAddress address, int port)
    {
        addServerPrivate(address, port);
    }

    private ServerRecord addServerPrivate(InetAddress address, int port)
    {
        ServerRecord newServer = new ServerRecord(address, port);

        lock.writeLock().lock();
        for(int i = 0; i < VNodes; i++)
        {
            VNode vnode = new VNode(newServer, i);
            ring.put(vnode.getHash(), vnode);
        }
        lock.writeLock().unlock();

        return newServer;
    }

    public void removeServer(InetAddress address, int port)
    {
        ServerRecord r = new ServerRecord(address, port);
        removeServer(r);
    }

    /**
     * This is now the standard method to remove servers
     * @param r server record
     */
    public void removeServer(ServerRecord r)
    {
        lock.writeLock().lock();
        for(int i = 0; i < VNodes; i++)
        {
            int hashcode = new VNode(r, i).getHash();
            ring.remove(hashcode);
        }
        lock.writeLock().unlock();
    }

    public ServerRecord getServer(byte[] key) throws NoServersException, NoSuchAlgorithmException {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        int hashcode = getHash(key);

        Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return new ServerRecord(server.getValue().getServerRecordClone());
    }

    public ServerRecord getRandomServer() throws NoServersException
    {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }

        int hashcode = new Random().nextInt();

        Map.Entry<Integer, VNode> server = ring.ceilingEntry(hashcode);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        lock.readLock().unlock();

        return server.getValue().getServerRecordClone();
    }

    public ServerRecord getNextServer() throws NoServersException {
        lock.readLock().lock();
        if(ring.isEmpty())
        {
            lock.readLock().unlock();
            throw new NoServersException();
        }


        Map.Entry<Integer, VNode> server = ring.ceilingEntry(current);
        /* Deal with case where the successor of the key is past "0" */
        server = (server == null) ? ring.firstEntry(): server;

        /* Set the ptr so that next ceiling entry will be the following node in the ring */
        current = server.getKey() + 1;

        lock.readLock().unlock();

        return server.getValue().getServerRecordClone();
    }

    public void setServerAlive(ServerRecord r)
    {
        lock.writeLock().lock();
        int hashcode = new VNode(r, 0).getHash();
        ring.get(hashcode).serverRecord.setLastSeenNow();
        lock.writeLock().unlock();
    }

    public void setServerDeadNow(ServerRecord r)
    {
        lock.writeLock().lock();
        int hashcode = new VNode(r, 0).getHash();
        ring.get(hashcode).serverRecord.setLastSeenDeadNow();
        lock.writeLock().unlock();
    }

    public void setServerInformationTime(ServerRecord r, long informationTime)
    {
        lock.writeLock().lock();
        int hashcode = new VNode(r, 0).getHash();
        ring.get(hashcode).serverRecord.setInformationTime(informationTime);
        lock.writeLock().unlock();
    }

    public void setServerStatusCode(ServerRecord r, int statusCode)
    {
        lock.writeLock().lock();
        int hashcode = new VNode(r, 0).getHash();
        ring.get(hashcode).serverRecord.setCode(statusCode);
        lock.writeLock().unlock();
    }

    private int getHash(byte[] key) throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        byte[] dig = md5.digest(key);

        return (
                (dig[3] & 0xFF) << 24 |
                (dig[2] & 0xFF) << 16 |
                (dig[1] & 0xFF) << 8 |
                (dig[0] & 0xFF)
                );
    }
    /**
     * returns whether the server exist in the ring
     * @param addr: the ip address of the server
     * @param port: the port of the server
     * @return whether the server exist in the ring
     */
    public boolean hasServer(InetAddress addr, int port){
        long hashcode = new VNode(new ServerRecord(addr, port), 0).getHash();
        lock.readLock().lock();
        boolean hasKey = ring.containsKey(hashcode);
        lock.readLock().unlock();
        return hasKey;
    }

    public static class NoServersException extends IllegalStateException {}
    class ServerDoesNotExistException extends IllegalStateException {};


    static class VNode {
        private ServerRecord serverRecord;
        private int vnodeID;
        private int hash;

        public VNode(ServerRecord physicalServer, int vnodeID)
        {
            this.serverRecord = physicalServer;
            this.vnodeID = vnodeID;

            /* Compute the hash */
            this.hash = genHashFromServer(physicalServer, vnodeID);
        }

        public ServerRecord getServerRecordClone() {
            return new ServerRecord(serverRecord);
        }

        private int genHashFromServer(ServerRecord record, int vnodeID)
        {
            int adrLen = record.getAddress().getAddress().length;
            ByteBuffer hashBuf = ByteBuffer.allocate(adrLen + (Integer.BYTES * 2));
            hashBuf.put(record.getAddress().getAddress());
            hashBuf.putInt(record.getPort());
            hashBuf.putInt(vnodeID);
            hashBuf.flip();
            return genHash(hashBuf.array());
        }

        public int getHash() {
            return hash;
        }

        private int genHash(byte[] key) {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            md5.reset();

            byte[] dig = md5.digest(key);

            return hash = (
                    (dig[3] & 0xFF) << 24 |
                            (dig[2] & 0xFF) << 16 |
                            (dig[1] & 0xFF) << 8 |
                            (dig[0] & 0xFF)
            );
        }

}

}

