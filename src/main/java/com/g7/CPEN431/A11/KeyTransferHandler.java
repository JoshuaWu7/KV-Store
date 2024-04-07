package com.g7.CPEN431.A11;

import com.g7.CPEN431.A11.client.KVClient;
import com.g7.CPEN431.A11.consistentMap.ConsistentMap;
import com.g7.CPEN431.A11.consistentMap.ForwardList;
import com.g7.CPEN431.A11.consistentMap.ServerRecord;
import com.g7.CPEN431.A11.map.KeyWrapper;
import com.g7.CPEN431.A11.map.ValueWrapper;
import com.g7.CPEN431.A11.newProto.KVRequest.KVPair;
import com.g7.CPEN431.A11.newProto.KVRequest.PutPair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import static com.g7.CPEN431.A11.KVServer.BULKPUT_MAX_SZ;
import static com.g7.CPEN431.A11.KVServer.self;

public class KeyTransferHandler  extends TimerTask{
    ReadWriteLock mapLock;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    AtomicInteger bytesUsed;
    ConsistentMap serverRing;
    ConcurrentLinkedQueue pendingRecordDeaths;
    Semaphore pendingUpdate;

    public KeyTransferHandler(ReadWriteLock mapLock,
                              ConcurrentMap<KeyWrapper, ValueWrapper> map,
                              AtomicInteger bytesUsed, ConsistentMap serverRing,
                              ConcurrentLinkedQueue pendingRecordDeaths,
                              Semaphore pendingUpdate) {
        this.mapLock = mapLock;
        this.map = map;
        this.bytesUsed = bytesUsed;
        this.serverRing = serverRing;
        this.pendingRecordDeaths = pendingRecordDeaths;
        this.pendingUpdate = pendingUpdate;
    }

    @Override
    public void run() {
        transferKeys();
    }


    private void transferKeys() {
        System.out.println("Starting key transfer. @" + self.getPort());

        mapLock.writeLock().lock();

        Collection<ForwardList> toBeForwarded = serverRing.getEntriesToBeForwarded(this.map.entrySet());
        pendingUpdate.release();

        mapLock.writeLock().unlock();



        byte[] byteArr = new byte[16384];
        KVClient sender = new KVClient(byteArr, 10, 2);
        Set<KVPair> toDelete = new HashSet<>();
        AtomicBoolean clearFlag = new AtomicBoolean(false);

        toBeForwarded.forEach((forwardList -> {
            if(clearFlag.get()) return;
            if(forwardList.getKeyEntries().size() > 50000)
            {
                mapLock.writeLock().lock();
                map.clear();
                clearFlag.set(true);
                mapLock.writeLock().unlock();
                return;
            }
            ServerRecord target = forwardList.getDestination();
            sender.setDestination(target.getAddress(), target.getPort());
            try {
                List<PutPair> temp = new ArrayList<>();
                int currPacketSize = 0;
                for (PutPair pair : forwardList.getKeyEntries()) {
                    //take an "engineering" approximation, because serialization is expensive
                    int pairLen = pair.getKey().length + pair.getValue().length + Integer.BYTES;

                    //clear the outgoing buffer and send the packet
                    if(currPacketSize + pairLen >= BULKPUT_MAX_SZ)
                    {
                        sender.setDestination(target.getAddress(), target.getPort());
                        sender.bulkPutPump(temp);
                        temp.clear();
                        currPacketSize = 0;
                    }
                    //add to the buffer.
                    temp.add(pair);
                    if(serverRing.getRtype(pair.getKey()) == ConsistentMap.RTYPE.UNR) toDelete.add((KVPair) pair);
                    currPacketSize += pairLen;

                }
                //clear the buffer.
                if(temp.size() > 0) sender.bulkPutPump(temp);
            } catch (KVClient.ServerTimedOutException e) {
                // TODO: Probably a wise idea to redirect the keys someplace else, but that is a problem for future me.
                System.out.println("Bulk transfer timed out. Marking recipient as dead.");
                return;
            } catch (KVClient.MissingValuesException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }));

        toDelete.forEach((pair) ->
        {
            ValueWrapper v = map.remove(new KeyWrapper(pair.getKey()));
            if(v != null) bytesUsed.addAndGet(-pair.getValue().length);
        });

        System.out.println("ending key transfer. @" + self.getPort());

    }
}
