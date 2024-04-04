package com.g7.CPEN431.A7.consistentMap;

import com.g7.CPEN431.A7.map.KeyWrapper;
import com.g7.CPEN431.A7.map.ValueWrapper;
import com.g7.CPEN431.A7.newProto.KVRequest.KVPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class ForwardList {
    ServerRecord destination;
    Collection<KVPair> keyEntries;

    public ForwardList(ServerRecord destination) {
        this.destination = destination;
        this.keyEntries = new ArrayList<KVPair>();
    }

    public void addToList(Map.Entry<KeyWrapper, ValueWrapper> entry)
    {
        keyEntries.add(new KVPair(entry.getKey().getKey(), entry.getValue().getValue(), entry.getValue().getVersion()));
    }

    public void addToList(KVPair pair)
    {
        keyEntries.add(pair);
    }

    public ServerRecord getDestination() {
        return destination;
    }

    public Collection<KVPair> getKeyEntries() {
        return keyEntries;
    }
}
