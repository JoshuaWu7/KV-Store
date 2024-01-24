package com.s82033788.CPEN431.A4;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ByteArrayFactory implements PooledObjectFactory {
    @Override
    public void activateObject(PooledObject pooledObject) throws Exception {
        //nothing to do here
    }

    @Override
    public void destroyObject(PooledObject pooledObject) throws Exception {
        //nothing to do here, let GC handle
    }

    @Override
    public PooledObject makeObject() throws Exception {
        return new DefaultPooledObject(new byte[KVServer.PACKET_MAX]);
    }

    @Override
    public void passivateObject(PooledObject pooledObject) throws Exception {
        //do nothing;

    }

    @Override
    public boolean validateObject(PooledObject pooledObject) {
        return true;
    }
}
