package com.g7.CPEN431.A7.newProto.KVRequest;

import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.newProto.shared.MessageFactory;

public class ServerEntryFactory implements MessageFactory {
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("ServerEntry")) return new ServerRecord();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
