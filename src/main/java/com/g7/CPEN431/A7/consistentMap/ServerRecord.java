package com.g7.CPEN431.A7.consistentMap;

import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Objects;

public class ServerRecord implements ServerEntry {
    private InetAddress address;
    private int port;
    private long hash;
    private long informationTime;
    private int updateCode;


    private boolean portExists = false;
    private boolean informationTimeExists = false;
    private boolean updateCodeExists = false;

    public ServerRecord(InetAddress address, int port, int vnode_id) {
        this.address = address;
        this.port = port;
        this.portExists = true;
        this.informationTime = Instant.now().toEpochMilli();
        this.informationTimeExists = true;
        this.updateCode = 1;
        this.updateCodeExists = true;

        byte[] adr = address.getAddress();

        ByteBuffer hashBuf = ByteBuffer.allocate(adr.length + (Integer.BYTES * 2));
        hashBuf.put(adr);
        hashBuf.putInt(port);
        hashBuf.putInt(vnode_id);
        hashBuf.flip();

        this.hash = genHash(hashBuf.array());
    }

    public ServerRecord() {
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

    @Override
    public boolean hasServerAddress() {
        return address != null;
    }

    @Override
    public byte[] getServerAddress() {
        return address.getAddress();
    }

    @Override
    public void setServerAddress(byte[] serverAddress) {
        try {
            this.address = InetAddress.getByAddress(serverAddress);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean hasServerPort() {
        return this.portExists;
    }

    @Override
    public int getServerPort() {
        return this.port;
    }

    @Override
    public void setServerPort(int serverPort) {
        this.port = serverPort;
        this.portExists = true;
    }

    @Override
    public boolean hasInformationTime() {
        return this.informationTimeExists;
    }

    @Override
    public long getInformationTime() {
        return this.informationTime;
    }

    @Override
    public void setInformationTime(long informationTime) {
        this.informationTime = informationTime;
        this.informationTimeExists = true;
    }

    @Override
    public boolean hasCode() {
        return updateCodeExists;
    }

    @Override
    public int getCode() {
        return updateCode;
    }

    @Override
    public void setCode(int code) {
        this.updateCode = code;
    }

    public class HashNotGeneratedException extends Exception {}
}
