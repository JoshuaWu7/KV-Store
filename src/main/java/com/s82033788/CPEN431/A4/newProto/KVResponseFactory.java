package com.s82033788.CPEN431.A4.newProto;

import com.s82033788.CPEN431.A4.wrappers.UnwrappedPayload;

public class KVResponseFactory implements MessageFactory{
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVResponse")) return new UnwrappedPayload();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}