package com.g7.CPEN431.A7.map;

import net.openhft.chronicle.bytes.BytesMarshallable;

public class ValueWrapper implements BytesMarshallable {


    /* TODO Warning, do not mutate*/
    private final byte[] value;
    private final int version;

    public ValueWrapper(byte[] value, int version) {
        this.value = value;
        this.version = version;
    }
    public byte[] getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

}
