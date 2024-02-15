package com.s82033788.CPEN431.A4.consistentMap;

import java.net.InetAddress;
import java.util.Objects;

public class ServerRecord {
    private InetAddress address;
    private int port;

    public ServerRecord(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerRecord that = (ServerRecord) o;
        return port == that.port && Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }
}
