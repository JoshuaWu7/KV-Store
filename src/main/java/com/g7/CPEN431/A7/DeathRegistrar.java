package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DeathRegistrar extends TimerTask {
    Map<ServerRecord, ServerRecord> deathRecord;
    ConcurrentLinkedQueue<ServerRecord> pendingRecords;
    ConsistentMap ring;
    KVClient sender;
    Random random;
    final static int K = 6;

    public DeathRegistrar(ConcurrentLinkedQueue<ServerRecord> pendingRecords, ConsistentMap ring)
    throws IOException {
        this.deathRecord = new HashMap<>();
        this.pendingRecords = pendingRecords;
        this.ring = ring;
        this.sender = new KVClient(null, 0, new DatagramSocket(), new byte[16384]);
        this.random = new Random();
    }

    @Override
    public void run() {
        updateDeathRecords();
        checkIsAlive();

    }

    private void updateDeathRecords()
    {
        ServerRecord n;
        while((n = pendingRecords.poll()) != null)
        {
            ServerRecord existing = deathRecord.get(n);

            if(!existing.hasInformationTime() || !n.hasInformationTime())
            {
                throw new IllegalStateException("Broadcast message has no information time");
            }

            if(existing != null && existing.getInformationTime() > n.getInformationTime())
            {
                continue;
            }

            deathRecord.put(n,n);
        }
    }

    private void gossip()
    {
        ServerRecord target;
        try {
            target = ring.getRandomServer();
        } catch (ConsistentMap.NoServersException e) {
            System.err.println("no servers to gossip with");
            return;
        }

        if(!target.hasServerAddress() || !target.hasServerPort())
        {
            throw new IllegalStateException();
        }

        if(deathRecord.size() == 0)
        {
            return;
        }

        sender.setDestination(target.getAddress(), target.getServerPort());
        List<ServerEntry> l = new ArrayList<>(deathRecord.values());
        ServerResponse r;
        try {
            r = sender.isDead(l);
        } catch (Exception e) {
            System.err.println("Spreading gossip failed");
            e.printStackTrace();
            return;
        }

        if(r.getErrCode() != KVServerTaskHandler.RES_CODE_SUCCESS)
        {
            System.err.println("Gossip recipient returned error: " + r.getErrCode());
            return;
        }

        if(!r.hasServerStatusCode())
        {
            System.err.println("Gossip recipient did not return list of status codes");
        }

        List<Integer> responses = r.getServerStatusCode();

        if(responses.size() != l.size())
        {
            System.err.println("Gossip recipient's status size is not the same");
        }

        for(int i = 0; i < l.size(); i++)
        {
            if(responses.get(i) == KVServerTaskHandler.STAT_CODE_OLD && random.nextInt(K) == 0)
            {
                //delete the response
                deathRecord.remove(l.get(i));
                //TODO additional logic to store old "news" that is recirculating. probably let vanessa do this.
            }
        }
    }

    private int checkIsAlive()
    {
        // TODO vicky;
        return 0;
    }

}
