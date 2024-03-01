package com.g7.CPEN431.A7.newProto.KVRequest;

import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.newProto.shared.MessageFactory;
import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;

public class KVRequestFactory implements MessageFactory {
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVRequest")) return new UnwrappedPayload();
        if(fullMessageName.equals("ServerEntry")) return new ServerRecord(fullMessageName);
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
