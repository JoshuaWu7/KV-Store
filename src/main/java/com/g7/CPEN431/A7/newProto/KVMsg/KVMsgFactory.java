package com.g7.CPEN431.A7.newProto.KVMsg;

import com.g7.CPEN431.A7.newProto.shared.MessageFactory;
import com.g7.CPEN431.A7.wrappers.UnwrappedMessage;

public class KVMsgFactory implements MessageFactory {

    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVMsg")) return new UnwrappedMessage();
        throw new IllegalArgumentException("Unknown Message Name: " + fullMessageName);
    }
}
