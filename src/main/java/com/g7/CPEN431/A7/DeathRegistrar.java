package com.g7.CPEN431.A7;

import com.g7.CPEN431.A7.client.KVClient;
import com.g7.CPEN431.A7.client.ServerResponse;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;

import java.io.IOException;
import java.net.DatagramSocket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.g7.CPEN431.A7.KVServer.*;
import static com.g7.CPEN431.A7.consistentMap.ServerRecord.CODE_DED;

public class DeathRegistrar extends TimerTask {
    Map<ServerRecord, ServerRecord> deathRecord;
    ConcurrentLinkedQueue<ServerRecord> pendingRecords;
    ConsistentMap ring;
    KVClient sender;
    Random random;
    ServerRecord self;
    long previousPingSendTime;
    final static int SUSPENDED_THRESHOLD = 300;
    final static int K = 6;

    public DeathRegistrar(ConcurrentLinkedQueue<ServerRecord> pendingRecords, ConsistentMap ring, ServerRecord self)
    throws IOException {
        this.deathRecord = new HashMap<>();
        this.pendingRecords = pendingRecords;
        this.ring = ring;
        this.sender = new KVClient(null, 0, new DatagramSocket(), new byte[16384]);
        this.random = new Random();
        this.self = self;
        this.previousPingSendTime = -1;
    }

    @Override
    public void run() {
        updateDeathRecords();
        checkSelfSuspended();
        checkIsAlive();
        gossip();
    }

    private void updateDeathRecords()
    {
        ServerRecord n;
        while((n = pendingRecords.poll()) != null)
        {
            ServerRecord existing = deathRecord.get(n);

            if((existing != null && !existing.hasInformationTime()) || !n.hasInformationTime())
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

        if(deathRecord.isEmpty())
        {
            return;
        }

        sender.setDestination(target.getAddress(), target.getServerPort());
        List<ServerEntry> l = new ArrayList<>(deathRecord.values());
        ServerResponse r;
        try {
            r = sender.isDead(l);
        } catch (KVClient.ServerTimedOutException e)
        {
            System.out.println("gossipee is dead");
            ring.setServerDeadNow(target);
            ring.removeServer(target);
            deathRecord.put(target, target);
            return;
        } catch (Exception e) {
            System.out.println("Spreading gossip failed");
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
            return;
        }

        List<Integer> responses = r.getServerStatusCode();

        if(responses.size() != l.size())
        {
            System.err.println("Gossip recipient's status size is not the same");
            return;
        }


        for(int i = 0; i < l.size(); i++)
        {
            if(responses.get(i) == KVServerTaskHandler.STAT_CODE_OLD && random.nextInt(K) == 0)
            {
                //delete the response
                deathRecord.remove( (ServerRecord) l.get(i));
                //TODO additional logic to store old "news" that is recirculating. probably let vanessa do this.
            }
        }

        /* Mark the gosipee as alive */
        ring.setServerAlive(target);
    }

    /**
     * This function picks a random node from the server list and sends an isAlive request
     * to it. If there is no response after retries, we update the server list to account for
     * the "dead" server. Otherwise, do nothing.
     */
    private void checkIsAlive() {
        ServerRecord target = null;

        try {
            target = ring.getNextServer();

            /* omit this round if next server is equal to self */
            if(target.equals(selfLoopback) || target.equals(self))
            {
                return;
            }

            previousPingSendTime = Instant.now().toEpochMilli();

            sender.setDestination(target.getAddress(), target.getServerPort());
            sender.isAlive();
        } catch (ConsistentMap.NoServersException | IOException | KVClient.MissingValuesException |
                 InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KVClient.ServerTimedOutException e) {
            // Update server death time to current time, then add to list of deaths
            ring.setServerDeadNow(target);
            ring.removeServer(target);
            deathRecord.put(target, target);
        }
    }

    /**
     * This function checks if the current server has been suspended, i.e. the time since it last sent
     * a ping is greater than 800 ms. If so, updates the server to be alive so that it can be broadcasted
     * next time.
     *
     * This function does nothing if the current server has not yet sent a ping before.
     */
    private void checkSelfSuspended() {
        if (previousPingSendTime > 0) {
            long currentTime = Instant.now().toEpochMilli();

            if (previousPingSendTime - currentTime > GOSSIP_INTERVAL + SUSPENDED_THRESHOLD) {
                // TODO: check for self loopback
                self.setLastSeenNow();
                ring.setServerAlive(self);
            }
        }
    }
}
