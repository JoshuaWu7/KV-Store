package com.s82033788.CPEN431.A4;

public class UnwrappedPayload {
    int command;
    byte[] key;
    byte[] value;
    int version; //defaults to zero anyways, no logic

    boolean keyExists;
    boolean valueExists;
    boolean versionExists;

    public UnwrappedPayload(int command, byte[] key, byte[] value, int version, boolean keyExists, boolean valueExists, boolean versionExists) {
        this.command = command;
        this.key = key;
        this.value = value;
        this.version = version;
        this.keyExists = keyExists;
        this.valueExists = valueExists;
        this.versionExists = versionExists;
    }
}
