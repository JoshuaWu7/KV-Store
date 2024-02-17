package com.g7.CPEN431.A7.consistentMap;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public class ServerRecord {
    private InetAddress address;
    private int port;
    private long hash;

    public ServerRecord(InetAddress address, int port, int vnode_id) {
        this.address = address;
        this.port = port;

        byte[] adr = address.getAddress();

        ByteBuffer hashBuf = ByteBuffer.allocate(adr.length + (Integer.BYTES * 2));
        hashBuf.put(adr);
        hashBuf.putInt(port);
        hashBuf.putInt(vnode_id);
        hashBuf.flip();

        this.hash = genHash(hashBuf.array());
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public long getHash() {return hash;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerRecord that = (ServerRecord) o;

        boolean res = port == that.port && this.address.equals(that.address);

        return port == that.port && this.address.equals(that.address);
    }


    private long genHash(byte[] key) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md5.reset();

        byte[] dig = md5.digest(key);

        long hash = (
                (long) (dig[7] & 0xFF) << 56 |
                        (long) (dig[6] & 0xFF) << 48 |
                        (long) (dig[5] & 0xFF) << 40 |
                        (long) (dig[4] & 0xFF) << 32 |
                        (long) (dig[3] & 0xFF) << 24 |
                        (long) (dig[2] & 0xFF) << 16 |
                        (long) (dig[1] & 0xFF) << 8 |
                        (long) (dig[0] & 0xFF)
        );

        return hash;
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }

    public class HashNotGeneratedException extends Exception {}
}
