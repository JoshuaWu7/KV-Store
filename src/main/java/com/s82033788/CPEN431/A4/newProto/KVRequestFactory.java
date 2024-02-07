package com.s82033788.CPEN431.A4.newProto;

import com.s82033788.CPEN431.A4.wrappers.UnwrappedPayload;

public class KVRequestFactory implements MessageFactory{
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVRequest")) return new UnwrappedPayload();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
