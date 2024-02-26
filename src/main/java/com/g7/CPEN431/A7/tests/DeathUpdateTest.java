package com.g7.CPEN431.A7.tests;

import com.g7.CPEN431.A7.KVServerTaskHandler;
import com.g7.CPEN431.A7.consistentMap.ConsistentMap;
import com.g7.CPEN431.A7.consistentMap.ServerRecord;
import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;
import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.g7.CPEN431.A7.KVServerTaskHandler.STAT_CODE_NEW;
import static com.g7.CPEN431.A7.KVServerTaskHandler.STAT_CODE_OLD;
import static org.junit.jupiter.api.Assertions.*;

public class DeathUpdateTest {
    static UnwrappedPayload payload = new UnwrappedPayload();
    static ConsistentMap ring;
    static final int num_vnode = 7;
    static List<InetAddress> server_addrs = new ArrayList<>();

    private static KVServerTaskHandler taskHandler;

    static ConcurrentLinkedQueue<ServerRecord> pendingRecordDeaths;

    @BeforeEach
    @DisplayName("setup")
    public void setup() throws UnknownHostException {
        try {
            ring = new ConsistentMap(num_vnode, "servers.txt");
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        pendingRecordDeaths = new ConcurrentLinkedQueue<>();
        taskHandler = new KVServerTaskHandler(ring, pendingRecordDeaths);

        for (int i = 0; i < num_vnode; i++) {
            InetAddress addr = InetAddress.getByAddress(new byte[]{127,0,0,(byte)i});
            server_addrs.add(addr);
            if (i != 1) ring.addServer(addr, 200);
        }
    }

    @Test
    @DisplayName("Test stat code NEW")
    public void deathUpdateNews() throws UnknownHostException {
        List<ServerEntry> servers = new ArrayList<>();
        ServerEntry s1 = new ServerRecord(server_addrs.get(0), 200, 0);
        servers.add(s1);
        payload.setServerRecord(servers);

        List<Integer> codes = taskHandler.getDeathCodes(servers);

        assertEquals(codes.get(0), STAT_CODE_NEW);
        assertFalse(ring.hasServer(server_addrs.get(0), 200));
        assert pendingRecordDeaths.peek() != null;
        assertEquals(pendingRecordDeaths.peek(), s1);
    }

    @Test
    @DisplayName("Test stat code OLD NEWS")
    public void deathUpdateOld() throws UnknownHostException {
        List<ServerEntry> servers = new ArrayList<>();
        ServerEntry s1 = new ServerRecord(server_addrs.get(1), 200, 0);
        servers.add(s1);
        payload.setServerRecord(servers);

        List<Integer> codes = taskHandler.getDeathCodes(servers);
        assertEquals(codes.get(0), STAT_CODE_OLD);
    }
}
