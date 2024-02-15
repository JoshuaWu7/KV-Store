package com.s82033788.CPEN431.A4.wrappers;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ConsistentMap {
    private final TreeMap<Integer, InetAddress> ring;
    private final int vnodes;


    public ConsistentMap(int vnodes, String serverPathName) throws IOException {
        this.ring = new TreeMap<>();
        this.vnodes = vnodes;

        Path path = Paths.get(serverPathName);
        List<String> serverList = Files.readAllLines(path , StandardCharsets.UTF_8);

        for(String server : serverList)
        {
            InetAddress addr = InetAddress.getByName(server);
            for(int i = 0; i < vnodes; i++)
            {
                int hashcode = Objects.hash(addr, i);
                ring.put(hashcode, addr);
            }
        }
    }

    public void addServer(InetAddress address)
    {
        for(int i = 0; i < vnodes; i++)
        {
            int hashcode = Objects.hash(address, i);
            ring.put(hashcode, address);
        }
    }

    public void removeServer(InetAddress address)
    {
        for(int i = 0; i < vnodes; i++)
        {
            int hashcode = Objects.hash(address, i);
            ring.remove(hashcode);
        }
    }

    public InetAddress getServer(byte[] key)
    {
        if(ring.isEmpty()) return null;

        int hashcode = Arrays.hashCode(key);

        Map.Entry<Integer, InetAddress> server = ring.floorEntry(hashcode);

        //case where we have to wrap around the ring.
        if(server == null) return ring.firstEntry().getValue();
        //normal case
        return server.getValue();
    }




}
