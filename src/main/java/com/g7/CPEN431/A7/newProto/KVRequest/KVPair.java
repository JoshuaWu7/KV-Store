package com.g7.CPEN431.A7.newProto.KVRequest;

public class KVPair implements PutPair {
    byte[] key;
    byte[] value;
    public KVPair() {
    }

    @Override
    public boolean hasKey() {
        return key != null;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean hasValue() {
        return value != null;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public void setValue(byte[] value) {
        this.value = value;
    }
}
