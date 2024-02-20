package com.g7.CPEN431.A7.wrappers;

import com.g7.CPEN431.A7.newProto.KVRequest;

/**
 * A class than encapsulates the payload of an incoming requrest. Self Explanatory.
 */
public class UnwrappedPayload implements KVRequest {
    private int command;
    private byte[] key;
    private byte[] value;
    private byte[] serverAddress;
    private int serverPort;
    private long informationTime;
    private int version = 0; //defaults to zero anyways, no logic

    private boolean keyExists = false;
    private boolean valueExists = false;
    private boolean versionExists = false;
    private boolean serverPortExists = false;
    private boolean informationTimeExists = false;

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

    @Override
    public boolean hasServerAddress() {
        return serverAddress != null;
    }

    @Override
    public byte[] getServerAddress() {
        return serverAddress;
    }

    @Override
    public void setServerAddress(byte[] serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public boolean hasServerPort() {
        return serverPortExists;
    }

    @Override
    public int getServerPort() {
        return this.serverPort;
    }

    @Override
    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
        this.serverPortExists = true;
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
}
