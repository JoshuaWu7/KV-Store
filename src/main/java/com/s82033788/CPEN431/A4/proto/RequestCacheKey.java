package com.s82033788.CPEN431.A4.proto;

import java.util.Arrays;

public class RequestCacheKey {
    byte[] requestID;
    long crc;
    public RequestCacheKey(byte[] requestID, long crc) {
        this.requestID = requestID;
        this.crc = crc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestCacheKey that = (RequestCacheKey) o;
        return Arrays.equals(requestID, that.requestID);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(requestID);
    }
}
