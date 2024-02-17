package com.g7.CPEN431.A7.newProto;

import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;

public class KVRequestFactory implements MessageFactory{
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVRequest")) return new UnwrappedPayload();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
