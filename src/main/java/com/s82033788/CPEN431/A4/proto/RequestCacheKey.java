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
    public int hashCode() {
        return Arrays.hashCode(requestID);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) return true;
        if(!(obj instanceof RequestCacheKey)) return false;

        return Arrays.equals((byte[]) obj, requestID);
    }
}
