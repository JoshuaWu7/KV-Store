package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.protobuf.InvalidProtocolBufferException;
import com.s82033788.CPEN431.A4.cache.RequestCacheKey;
import com.s82033788.CPEN431.A4.cache.RequestCacheValue;
import com.s82033788.CPEN431.A4.map.KeyWrapper;
import com.s82033788.CPEN431.A4.map.ValueWrapper;
import com.s82033788.CPEN431.A4.newProto.*;
import com.s82033788.CPEN431.A4.wrappers.PB_ContentType;
import com.s82033788.CPEN431.A4.wrappers.PublicBuffer;
import com.s82033788.CPEN431.A4.wrappers.UnwrappedMessage;
import com.s82033788.CPEN431.A4.wrappers.UnwrappedPayload;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import static com.s82033788.CPEN431.A4.cache.ResponseType.*;

public class KVServerTaskHandler implements Runnable {
    private final AtomicInteger bytesUsed;
    private DatagramPacket iPacket;
    private DatagramSocket socket;
    private Cache<RequestCacheKey, DatagramPacket> requestCache;
    private ConcurrentMap<KeyWrapper, ValueWrapper> map;
    private ReadWriteLock mapLock;
    //this is concurrently inconsequential, they can just try again later.
    private boolean responseSent = false;
    private PublicBuffer incomingPublicBuffer;

    final private ConcurrentLinkedQueue bytePool;  //this is thread safe
    final private boolean isOverloaded;
    final private ConcurrentLinkedQueue<DatagramPacket> outbound;

    // Constants
    final static int KEY_MAX_LEN = 32;
    final static int VALUE_MAX_LEN = 10_000;
    final static int MEMORY_SAFETY = 524_288;
    public final static int RES_CODE_INVALID_KEY = 0x6;
    public final static int RES_CODE_INVALID_VALUE = 0x7;
    public final static int RES_CODE_INVALID_OPCODE = 0x5;
    public final static int RES_CODE_SUCCESS = 0x0;
    public final static int RES_CODE_INVALID_OPTIONAL = 0x21;
    public final static int RES_CODE_RETRY_NOT_EQUAL = 0x22;
    public final static int RES_CODE_NO_KEY = 0x01;
    public final static int RES_CODE_NO_MEM = 0x2;
    public final static int RES_CODE_OVERLOAD = 0x3;

    //parameters

    public final static int CACHE_OVL_WAIT_TIME = 80;
    public final static int THREAD_OVL_WAIT_TIME = 16;

    public KVServerTaskHandler(DatagramPacket iPacket,
                               DatagramSocket socket,
                               Cache<RequestCacheKey, DatagramPacket> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map,
                               ReadWriteLock mapLock,
                               AtomicInteger bytesUsed,
                               ConcurrentLinkedQueue<byte[]> bytePool,
                               boolean isOverloaded,
                               ConcurrentLinkedQueue<DatagramPacket> outbound) {
        this.iPacket = iPacket;
        this.socket = socket;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
        this.bytesUsed = bytesUsed;
        this.bytePool = bytePool;
        this.isOverloaded = isOverloaded;
        this.outbound = outbound;
    }


    @Override
    public void run()
    {
        try {
            mainHandlerFunction();
        } catch (Exception e) {
            System.err.println("Thread Crash");
            System.err.println(e);
            throw e;
        }
        finally {
            Arrays.fill(iPacket.getData(), (byte) 0);
            bytePool.offer(iPacket.getData());

        }
    }

    public void mainHandlerFunction() {
        if (responseSent) throw new IllegalStateException();

        //decode the message
        UnwrappedMessage unwrappedMessage;
        try {
            unwrappedMessage = unpackPacket(iPacket);
        } catch (IOException e) {
            System.err.println("Packet does not match .proto");
            System.err.println(e);
            System.err.println("Stopping packet handling and returning");

            //No response, like A1/A2
            return;
        } catch (InvalidChecksumException e) {
            System.err.println("Packet checksum does not match");
            System.err.println("Stopping packet handling and returning");

            //no response, like A1/A2
            return;
        }


       //TODO check cache space remaining
        DatagramPacket reply;
        try {
            reply = requestCache.get(new RequestCacheKey(unwrappedMessage.getMessageID(), unwrappedMessage.getCheckSum()),
                    () -> newProcessRequest(unwrappedMessage));
        } catch (ExecutionException e) {
            //TODO deal with this
            if(e.getCause() instanceof InvalidProtocolBufferException)
            {
                System.err.println("Unable to decode payload. Doing nothing");
            }
            else {
                System.err.println(e);
            }

            return;
        }

        //assert that the request is exactly the same - honestly none of my business and I don't wanna deal with this
        // anymore
//        if(reply.getIncomingCRC() != unwrappedMessage.getCrc()) {
//            try {
//                RequestCacheValue res = new RequestCacheValue.Builder(
//                        unwrappedMessage.getCrc(),
//                        iPacket.getAddress(),
//                        iPacket.getPort(),
//                        unwrappedMessage.getReqID(),
//                        incomingPublicBuffer)
//                        .setResponseType(RETRY_NOT_EQUAL)
//                        .build();
//
//                sendResponse(res.generatePacket());
//            } catch (IOException e) {
//                System.err.println("Unable to send mismatched retry due to IO");
//                System.err.println("Will not send response packet");
//            }
//            return;
//        }

        //send the reply, no concurrency problem here, since packet will be sent eventually.
        // (tbh don't even need to synchronize load other than to avoid double loading)
        if (!responseSent) {
            try {
                sendResponse(reply);
            } catch (IOException e) {
                System.err.println("Unable to send cached response due to IO");
                System.err.println("Will not send response packet");
            }
        }
    }

    private DatagramPacket newProcessRequest(UnwrappedMessage unwrappedMessage) throws
            IOException{

        UnwrappedPayload payload;
        RequestCacheValue.Builder scaf = new RequestCacheValue.Builder(
                unwrappedMessage.getCheckSum(),
                iPacket.getAddress(),
                iPacket.getPort(),
                unwrappedMessage.getMessageID(),
                incomingPublicBuffer);

        //verify overload condition
        if(isOverloaded) {
            //System.out.println("Cache overflow. Delay Requested");
            RequestCacheValue res = new RequestCacheValue.Builder(unwrappedMessage.getCheckSum(),
                    iPacket.getAddress(),
                    iPacket.getPort(),
                    unwrappedMessage.getMessageID(),
                    incomingPublicBuffer)
                    .setResponseType(OVERLOAD_THREAD)
                    .build();

            return generateAndSend(res);
        }

        payload = unpackPayload(incomingPublicBuffer);

        //overload
        //TODO replace cache to reference inside map, rather than whole packet.
//        if(requestCache.size() >= KVServer.CACHE_SZ){
//            System.out.println("Cache overflow. Delay Requested");
//            RequestCacheValue res = new RequestCacheValue.Builder(unwrappedMessage.getCrc(),
//                    iPacket.getAddress(),
//                    iPacket.getPort(),
//                    unwrappedMessage.getReqID(),
//                    incomingPublicBuffer)
//                    .setResponseType(OVERLOAD_CACHE)
//                    .build();
//
//            sendResponse(res.generatePacket());
//            return res;
//        }

//        if(tpe.getPoolSize() > 64)
//        {
//            System.out.println("Cache overflow. Delay Requested");
//            RequestCacheValue res = new RequestCacheValue.Builder(unwrappedMessage.getCrc(),
//                    iPacket.getAddress(),
//                    iPacket.getPort(),
//                    unwrappedMessage.getReqID(),
//                    incomingPublicBuffer)
//                    .setResponseType(OVERLOAD_THREAD)
//                    .build();
//
//            sendResponse(res.generatePacket());
//            return res;
//        }

        //process the packet by request code
        DatagramPacket res;
        switch(payload.getCommand())
        {
            case 0x01: res = handlePut(scaf, payload); break;
            case 0x02: res = handleGet(scaf, payload); break;
            case 0x03: res = handleDelete(scaf, payload); break;
            case 0x04: res = handleShutdown(scaf, payload); break;
            case 0x05: res = handleWipeout(scaf, payload);  break;
            case 0x06: res = handleIsAlive(scaf, payload); break;
            case 0x07: res =  handleGetPID(scaf, payload); break;
            case 0x08: res = handleGetMembershipCount(scaf, payload);  break;

            default: {
                RequestCacheValue val = scaf.setResponseType(INVALID_OPCODE).build();
                res =  generateAndSend(val);
            }
        }

        if(!responseSent) {
            System.out.println(payload.getCommand());
            throw new IllegalStateException();
        }
        return res;
    }


    //helper function to unpack packet
    private UnwrappedMessage unpackPacket(DatagramPacket iPacket)
            throws IOException, InvalidChecksumException {
        long expectedCRC;

        incomingPublicBuffer = new PublicBuffer(iPacket.getData(), PB_ContentType.PACKET, iPacket.getLength());

        KVMsg deserialized = KVMsgSerializer.parseFrom(new KVMsgFactory(),
                incomingPublicBuffer.readPacketFromPB());

        byte[] id = deserialized.getMessageID();
        byte[] pl = deserialized.getPayload();

        incomingPublicBuffer.writeIDToPB().write(id);
        incomingPublicBuffer.writePayloadToPBAfterID().write(pl);

        //verify checksum
        long actualCRC = incomingPublicBuffer.getCRCFromBody();

        if (actualCRC != deserialized.getCheckSum()) throw new InvalidChecksumException();

        return (UnwrappedMessage) deserialized;
    }

    // helper function to unpack payload
    private UnwrappedPayload unpackPayload(PublicBuffer payload) throws
            IOException{

        com.s82033788.CPEN431.A4.newProto.KVRequest deserialized = KVRequestSerializer.parseFrom(new KVRequestFactory(), payload.readPayloadFromPBBody());
        return (UnwrappedPayload) deserialized;
    }

    private void sendResponse(DatagramPacket d) throws IOException {
        if (responseSent) throw new IllegalStateException();

        responseSent = true;

        outbound.offer(d);

//        socket.send(d);
    }

    //helper functions to process requests


    private DatagramPacket handleGetMembershipCount(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        RequestCacheValue res = scaf
                .setResponseType(INVALID_OPTIONAL)
                .setMembershipCount(1)
                .build();

        return generateAndSend(res);
    }

    private DatagramPacket handleGetPID(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        long pid = ProcessHandle.current().pid();
        RequestCacheValue res = scaf
                .setResponseType(PID)
                .setPID(pid)
                .build();

        return generateAndSend(res);
    }


    private DatagramPacket handleIsAlive(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();
        return generateAndSend(res);
    }


    private DatagramPacket handleShutdown (RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }


        RequestCacheValue res = scaf.setResponseType(SHUTDOWN).build();
        DatagramPacket pkt = generateAndSend(res);

        System.out.println("Recevied shutdown command, shutting down now");
        System.exit(0);
        return pkt;
    }

    private DatagramPacket handlePut(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
    throws IOException {
        if(!payload.hasKey() || !payload.hasValue())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();

            return generateAndSend(res);
        }

        // no need to check version, since by default it is parsed as 0,
        // which according to the spec, is the default value we want

        //defensive design to reject 0 length keys
        if(payload.getKey().length <= 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        if(payload.getValue().length > VALUE_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_VALUE).build();
            return generateAndSend(res);
        }


        //check memory remaining
        long remainingMemory = Runtime.getRuntime().maxMemory() -
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

        //add additional 3000 elem size to deal with non gc'd items hogging up the memory.
        if (map.size() > 4800 && remainingMemory < MEMORY_SAFETY) {
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            return generateAndSend(res);
        }


        if(bytesUsed.get() >= 60_817_408) {
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            return generateAndSend(res);
        }

        mapLock.readLock().lock();

        //atomically put and respond, `tis thread safe.
        AtomicReference<IOException> ioexception= new AtomicReference<>();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            try {
                RequestCacheValue res = scaf.setResponseType(PUT).build();
                pkt.set(generateAndSend(res));
                bytesUsed.addAndGet(payload.getValue().length);
                return new ValueWrapper(payload.getValue(), payload.getVersion());
            } catch (IOException e) {
                ioexception.set(e);
                System.err.println(e);
                System.err.println("Socket exception when returning result of PUT");
                System.err.println("Reply packet will not be sent");
            }
            return value;
        });

        mapLock.readLock().unlock();

        if (ioexception.get() != null) throw ioexception.get();

        return pkt.get();
    }
    private DatagramPacket handleGet(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if((!payload.hasKey()) || payload.hasValue() || payload.hasVersion())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length <= 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        //atomically get and respond
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            try {
                RequestCacheValue res;
                if (value == null) {
                    res = scaf.setResponseType(NO_KEY).build();
                } else {
                    res = scaf
                            .setResponseType(VALUE)
                            .setValue(value)
                            .build();
                }
                pkt.set(generateAndSend(res));
                return value;
            } catch (IOException e) {
                System.err.println(e);
                System.err.println("Socket exception when returning result of GET");
                System.err.println("Reply packet will not be sent");
                ioexception.set(e);
            }
            return value;
        });
        mapLock.readLock().unlock();

        if (ioexception.get() != null) throw ioexception.get();

        return pkt.get();

    }

    private DatagramPacket handleDelete(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if((!payload.hasKey()) || payload.hasValue() || payload.hasVersion())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length <= 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        //atomically del and respond
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        mapLock.readLock().lock();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            try {
                if (value == null) {
                    RequestCacheValue res = scaf.setResponseType(NO_KEY).build();
                    pkt.set(generateAndSend(res));
                    return value;
                } else {
                    bytesUsed.addAndGet(-value.getValue().length);
                    RequestCacheValue res = scaf.setResponseType(DEL).build();
                    pkt.set(generateAndSend(res));
                    return null;
                }
            } catch (IOException e) {
                System.err.println(e);
                System.err.println("Socket exception when returning result of GET");
                System.err.println("Reply packet will not be sent");
                ioexception.set(e);
            }
            return value;
        });

        mapLock.readLock().unlock();
        if (ioexception.get() != null) throw ioexception.get();

        return pkt.get();
    }

    private DatagramPacket handleWipeout(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //atomically wipe and respond
        mapLock.writeLock().lock();
        map.clear();
        bytesUsed.set(0);
        RequestCacheValue res = scaf.setResponseType(WIPEOUT).build();
        DatagramPacket pkt = generateAndSend(res);
        mapLock.writeLock().unlock();

        System.gc();

        return pkt;
    }

    DatagramPacket generateAndSend(RequestCacheValue res) throws IOException {
        DatagramPacket pkt = res.generatePacket();
        sendResponse(pkt);
        return pkt;
    }


    // Custom Exceptions

    static class InvalidChecksumException extends Exception {}
    static class KeyLengthException extends Exception {}
    static class ValueLengthException extends Exception {}

}
