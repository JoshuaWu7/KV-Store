package com.g7.CPEN431.A11.newProto.KVMsg;

import com.g7.CPEN431.A11.newProto.shared.MessageFactory;
import com.g7.CPEN431.A11.wrappers.UnwrappedMessage;

public class KVMsgFactory implements MessageFactory {

    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVMsg")) return new UnwrappedMessage();
        throw new IllegalArgumentException("Unknown Message Name: " + fullMessageName);
    }
}
