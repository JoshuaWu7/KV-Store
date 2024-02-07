package com.s82033788.CPEN431.A4.wrappers;

import com.s82033788.CPEN431.A4.newProto.KVRequest;

public class UnwrappedPayload implements KVRequest {
    private int command;
    private byte[] key;
    private byte[] value;
    private int version = 0; //defaults to zero anyways, no logic

    private boolean keyExists = false;
    private boolean valueExists = false;
    private boolean versionExists = false;

    public UnwrappedPayload() {
    }

    @Override
    public boolean hasCommand() {
        return true;
    }

    public int getCommand() {
       return command;
    }

    @Override
    public void setCommand(int command) {
        this.command = command;
    }

    @Override
    public boolean hasKey() {
        return keyExists;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public void setKey(byte[] key) {
        this.key = key;
        keyExists = true;
    }

    @Override
    public boolean hasValue() {
        return valueExists;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public void setValue(byte[] value) {
        this.value = value;
        valueExists = true;
    }

    @Override
    public boolean hasVersion() {
        return versionExists;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
        versionExists = true;
    }
}
