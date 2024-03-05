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

/**
 * A map API for a consistent hashing scheme.
 */
public class ConsistentMap {
    private final TreeMap<Integer, VNode> ring;
    private final int VNodes;
    private final ReadWriteLock lock;
    private int current = 0;

    /**
     *
     * @param vNodes number of vnodes in the consistent hashing scheme
     * @param serverPathName path to txt file containing server IP addresses + port
     * @throws IOException if cannot read txt file.
     */
    public ConsistentMap(int vNodes, String serverPathName) throws IOException  {
        this.ring = new TreeMap<>();
        this.VNodes = vNodes;
        this.lock = new ReentrantReadWriteLock();

        // Parse the txt file with all servers.
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

    /**
     *
     * @param address IP address of server to add
     * @param port of the server to add
     */
    public void addServer(InetAddress address, int port)
    {
        addServerPrivate(address, port);
    }

    /**
     * Do not use this in public, since invariants are broken, leading to concurrency guarantees failing
     * @param address IP address of server to add
     * @param port of the server to add
     * @return The actual server record in the ring
     */
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

    /**
     * Has no effects if server does not exist
     * @param address IP address of server to remove
     * @param port of server to remove
     *
     */
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

    /**
     *
     * @param key - byte array containing the key of the KV pair that will need to be mapped to a server
     * @return A copy of the server
     * @throws NoServersException If there are no servers in the ring
     * @throws NoSuchAlgorithmException If MD5 hashing fails
     */
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

    /**
     *
     * @return A random server in the ring.
     * @throws NoServersException - If there are no servers
     */
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

    /**
     *
     * @return The next server, goes round robin starting from self.
     * @throws NoServersException If there are no servers in the ring
     */
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

    /**
     * Sets the corresponding physical server's record time to the current time,
     * and marks it as alive
     * @param r Server record (can be clone) who's record is to be amended.
     */
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

    public int getServerCount()
    {
        lock.readLock().lock();
        int sz = ring.size();
        lock.readLock().unlock();
        return sz;
    }

    /**
     * Helper function to hash any byte array to int
     * @param key The byte array
     * @return An integer hash
     * @throws NoSuchAlgorithmException
     */
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


    /**
     * A virtual node representing a physical server
     */
    static class VNode {
        private ServerRecord serverRecord;
        private int vnodeID;
        private int hash;

        /**
         * @param physicalServer - The server that the vnode wraps
         * @param vnodeID - An arbitrary int that differentiates vnodes of the same server apart
         */
        public VNode(ServerRecord physicalServer, int vnodeID)
        {
            this.serverRecord = physicalServer;
            this.vnodeID = vnodeID;

            /* Compute the hash */
            this.hash = genHashFromServer(physicalServer, vnodeID);
        }

        /**
         *
         * @return A clone of the server record wrapped.
         */
        public ServerRecord getServerRecordClone() {
            return new ServerRecord(serverRecord);
        }

        /**
         * Helper function to generate an integer hash from a server's ip + vnode id + port
         * @param record The wrapped physical server
         * @param vnodeID Unique ID representing vnode
         * @return integer hash
         */
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

        /**
         *
         * @return the unique hash of this vnode
         */
        public int getHash() {
            return hash;
        }

        /**
         * Helper function to generate integer hash from byte array using MD5
         * @param key byte array
         * @return integer hash
         */
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

