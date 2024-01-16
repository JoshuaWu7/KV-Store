package com.s82033788.CPEN431.A4;

public class UnwrappedPayload {
    int command;
    byte[] key;
    byte[] value;
    int version;


    public UnwrappedPayload(int command, byte[] key, byte[] value, int version) {
        this.command = command;
        this.key = key;
        this.value = value;
        this.version = version;
    }
}
