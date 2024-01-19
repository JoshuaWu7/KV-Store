package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.s82033788.CPEN431.A4.proto.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.zip.CRC32;

import static com.s82033788.CPEN431.A4.proto.ResponseType.*;

public class KVServerTaskHandler implements Runnable {
    DatagramPacket iPacket;
    DatagramSocket socket;
    Cache<RequestCacheKey, RequestCacheValue> requestCache;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    ReadWriteLock mapLock;
    ThreadPoolExecutor tpe; //this is concurrently inconsequential, they can just try again later.
    //DatagramPacket outgoingResponse;
    boolean responseSent = false;

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
    public final static int THREAD_OVL_WAIT_TIME = 10;

    public KVServerTaskHandler(DatagramPacket iPacket,
                               DatagramSocket socket,
                               Cache<RequestCacheKey, RequestCacheValue> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock mapLock,
                               ThreadPoolExecutor tpe) {
        this.iPacket = iPacket;
        this.socket = socket;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
        this.tpe = tpe;
    }


    @Override
    public void run() {
        //TODO perform memory and cache runtime checks

        //cache might have data race, be alert.
        if (responseSent) throw new IllegalStateException();

        //decode the message
        UnwrappedMessage unwrappedMessage;
        try {
            unwrappedMessage = unpackPacket(iPacket);
        } catch (InvalidProtocolBufferException e) {
            //System.err.println("Packet does not match .proto");
            //System.err.println(e);
            //System.err.println("Stopping packet handling and returning");

            //No response, like A1/A2
            return;
        } catch (InvalidChecksumException e) {
            //System.err.println("Packet checksum does not match");
            //System.err.println("Stopping packet handling and returning");

            //no response, like A1/A2
            return;
        }


       //TODO check cache space remaining
        RequestCacheValue reply;
        try {
            reply = requestCache.get(new RequestCacheKey(unwrappedMessage.reqID.toByteArray(), unwrappedMessage.crc),
                    () -> newProcessRequest(unwrappedMessage));
        } catch (ExecutionException e) {
            //TODO deal with this
            throw new RuntimeException(e);
        }

        //assert that the request is exactly the same
        if(reply.getIncomingCRC() != unwrappedMessage.crc) {
            try {
                RequestCacheValue res = new RequestCacheValue.Builder(
                        unwrappedMessage.crc,
                        iPacket.getAddress(),
                        iPacket.getPort(),
                        unwrappedMessage.reqID)
                        .setResponseType(RETRY_NOT_EQUAL)
                        .build();

                sendResponse(res.generatePacket());
            } catch (IOException e) {
                //System.err.println("Unable to send mismatched retry due to IO");
                //System.err.println("Will not send response packet");
            }
            return;
        }

        //send the reply, no concurrency problem here, since packet will be sent eventually.
        // (tbh don't even need to synchronize load other than to avoid double loading)
        if (!responseSent) {
            try {
                sendResponse(reply.generatePacket());
            } catch (IOException e) {
                //System.err.println("Unable to send cached response due to IO");
                //System.err.println("Will not send response packet");
            }
            return;
        }
    }

    private RequestCacheValue newProcessRequest(UnwrappedMessage unwrappedMessage) throws
            IOException {


        UnwrappedPayload payload;
        RequestCacheValue.Builder scaf = new RequestCacheValue.Builder(
                unwrappedMessage.crc,
                iPacket.getAddress(),
                iPacket.getPort(),
                unwrappedMessage.reqID);

        try {
            payload = unpackPayload(unwrappedMessage.payload);
        }  catch (ValueLengthException e) {
            //System.err.println("Value field exceeds size");
            //System.err.println("Sending rejection packet");
            RequestCacheValue res;
            scaf.setResponseType(INVALID_VALUE);
            res = scaf.build();
            sendResponse(res.generatePacket());
            return res;
        } catch (KeyLengthException e) {
            //System.err.println("Value field exceeds size");
            //System.err.println("Sending rejection packet");

            scaf.setResponseType(INVALID_KEY);
            RequestCacheValue res = scaf.build();
            sendResponse(res.generatePacket());
            return res;
        }

        //overload
        //TODO replace cache to reference inside map, rather than whole packet.
        if(requestCache.size() >= KVServer.CACHE_SZ){
            System.out.println("Cache overflow. Delay Requested");
            RequestCacheValue res = new RequestCacheValue.Builder(unwrappedMessage.crc,
                    iPacket.getAddress(),
                    iPacket.getPort(),
                    unwrappedMessage.reqID)
                    .setResponseType(OVERLOAD_CACHE)
                    .build();

            sendResponse(res.generatePacket());
            return res;
        }
////
//        if(tpe.getActiveCount() - 1 >= tpe.getMaximumPoolSize())
//        {
//            DatagramPacket responsePacket;
//            responsePacket = replyMsg(unwrappedMessage.reqID, replyOverloadThreads());
//            return new RequestCacheValue(responsePacket, unwrappedMessage.crc);
//        }

        //process the packet by request code
        ByteString reqID = unwrappedMessage.reqID;
        RequestCacheValue res;
        switch(payload.command)
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
                res = scaf.setResponseType(INVALID_OPCODE).build();
                sendResponse(res.generatePacket());
            }
        }

        if(!responseSent) throw new IllegalStateException();
        return res;
    }


    //helper function to unpack packet
    private UnwrappedMessage unpackPacket(DatagramPacket iPacket)
            throws InvalidProtocolBufferException, InvalidChecksumException {
        long expectedCRC;
        ByteString payload;
        ByteString messageID;


        byte[] trimmedMsg = Arrays.copyOf(iPacket.getData(), iPacket.getLength());
        Message.Msg deserialized = Message.Msg.parseFrom(trimmedMsg);
        expectedCRC = deserialized.getCheckSum();
        payload = deserialized.getPayload();
        messageID = deserialized.getMessageID();

        //verify checksum
        byte[] allBody = messageID.concat(payload).toByteArray();
        CRC32 crc32 = new CRC32();
        crc32.update(allBody);
        long actualCRC = crc32.getValue();
        if (actualCRC != expectedCRC) throw new InvalidChecksumException();

        return new UnwrappedMessage(messageID, payload, actualCRC);
    }

    // helper function to unpack payload
    private UnwrappedPayload unpackPayload(ByteString payload) throws
            InvalidProtocolBufferException, KeyLengthException, ValueLengthException {
        KeyValueRequest.KVRequest deserialized = KeyValueRequest.KVRequest.parseFrom(payload);
        int command = deserialized.getCommand();
        byte[] key = deserialized.getKey().toByteArray();
        byte[] value = deserialized.getValue().toByteArray();
        int version = deserialized.getVersion();

        if(key.length > KEY_MAX_LEN) throw new KeyLengthException();
        if(value.length > VALUE_MAX_LEN) throw new ValueLengthException();

        return new UnwrappedPayload(command,
                key,
                value,
                version,
                deserialized.hasKey(),
                deserialized.hasValue(),
                deserialized.hasVersion() );
    }

    private void sendResponse(DatagramPacket d) throws IOException {
        if (responseSent) throw new IllegalStateException();

        responseSent = true;
        socket.send(d);
    }


    //helper function to send a packet given payload and id. Mutates outgoingResponse
//    private DatagramPacket replyMsg(ByteString id, ByteString payload) throws IOException {
//        //check that response to this incoming packet has not been sent
//        if(outgoingResponse != null) throw new IllegalStateException();
//
//        //prepare checksum
//        byte[] fullBody = id.concat(payload).toByteArray();
//        CRC32 crc32 = new CRC32();
//        crc32.update(fullBody);
//        long msgChecksum = crc32.getValue();
//
//        //prepare message
//        byte[] msg = Message.Msg.newBuilder()
//                    .setMessageID(id)
//                    .setPayload(payload)
//                    .setCheckSum(msgChecksum)
//                    .build()
//                    .toByteArray();
//
//        DatagramPacket pkt = new DatagramPacket(msg, msg.length, iPacket.getAddress(), iPacket.getPort());
//        socket.send(pkt);
//        outgoingResponse = pkt;
//        return pkt;
//    }

    //helper functions to process requests


    private RequestCacheValue handleGetMembershipCount(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        RequestCacheValue res = scaf
                .setResponseType(INVALID_OPTIONAL)
                .setMembershipCount(1)
                .build();
        sendResponse(res.generatePacket());
        return res;
    }

    private RequestCacheValue handleGetPID(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        long pid = ProcessHandle.current().pid();
        RequestCacheValue res = scaf
                .setResponseType(PID)
                .setPID(pid)
                .build();
        sendResponse(res.generatePacket());
        return res;
    }


    private RequestCacheValue handleIsAlive(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();
        sendResponse(res.generatePacket());
        return res;
    }


    private RequestCacheValue handleShutdown (RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }


        RequestCacheValue res = scaf.setResponseType(SHUTDOWN).build();
        sendResponse(res.generatePacket());

        System.out.println("Recevied shutdown command, shutting down now");
        System.exit(0);
        return res;
    }

    private RequestCacheValue handlePut(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
    throws IOException {
        if(!payload.keyExists || !payload.valueExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        // no need to check version, since by default it is parsed as 0,
        // which according to the spec, is the default value we want

        //defensive design to reject 0 length keys
        if(payload.key.length <= 0)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            sendResponse(res.generatePacket());
            return res;
        }


        //check memory remaining
        long remainingMemory = Runtime.getRuntime().maxMemory() -
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

        //add additional 3000 elem size to deal with non gc'd items hogging up the memory.
        if (map.size() > 4800 && remainingMemory < MEMORY_SAFETY) {
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically put and respond, `tis thread safe.
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception= new AtomicReference<>();
        AtomicReference<RequestCacheValue> res = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.key), (key, value) -> {
            try {
                res.set(scaf.setResponseType(PUT).build());
                sendResponse(res.get().generatePacket());
                return new ValueWrapper(payload.value, payload.version);
            } catch (IOException e) {
                ioexception.set(e);
                //System.err.println(e);
                //System.err.println("Socket exception when returning result of PUT");
                //System.err.println("Reply packet will not be sent");
            }
            return value;
        });

        mapLock.readLock().unlock();

        if (ioexception.get() != null) throw ioexception.get();

        return res.get();
    }
    private RequestCacheValue handleGet(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if((!payload.keyExists) || payload.valueExists || payload.versionExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //defensive design to reject 0 length keys
        if(payload.key.length <= 0)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically get and respond
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        AtomicReference<RequestCacheValue> res = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.key), (key, value) -> {
            try {
                if (value == null) {
                    res.set(scaf.setResponseType(NO_KEY).build());
                    sendResponse(res.get().generatePacket());
                    return value;
                } else {
                    res.set(scaf
                            .setResponseType(VALUE)
                            .setValue(value)
                            .build());
                    sendResponse(res.get().generatePacket());
                    return value;
                }
            } catch (IOException e) {
                //System.err.println(e);
                //System.err.println("Socket exception when returning result of GET");
                //System.err.println("Reply packet will not be sent");
                ioexception.set(e);
            }
            return value;
        });
        mapLock.readLock().unlock();

        if (ioexception.get() != null) throw ioexception.get();

        return res.get();
    }

    private RequestCacheValue handleDelete(RequestCacheValue.Builder scaf, UnwrappedPayload payload) throws IOException {
        if((!payload.keyExists) || payload.valueExists || payload.versionExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //defensive design to reject 0 length keys
        if(payload.key.length <= 0)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically del and respond
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        AtomicReference<RequestCacheValue> res = new AtomicReference<>();
        mapLock.readLock().lock();
        map.compute(new KeyWrapper(payload.key), (key, value) -> {
            try {
                if (value == null) {
                    res.set(scaf.setResponseType(NO_KEY).build());
                    sendResponse(res.get().generatePacket());
                    return value;
                } else {
                    res.set(scaf.setResponseType(DEL).build());
                    sendResponse(res.get().generatePacket());
                    return null;
                }
            } catch (IOException e) {
                //System.err.println(e);
                //System.err.println("Socket exception when returning result of GET");
                //System.err.println("Reply packet will not be sent");
                ioexception.set(e);
            }
            return value;
        });

        mapLock.readLock().unlock();
        if (ioexception.get() != null) throw ioexception.get();

        return res.get();
    }

    private RequestCacheValue handleWipeout(RequestCacheValue.Builder scaf, UnwrappedPayload payload)
            throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            sendResponse(res.generatePacket());
            return res;
        }

        //atomically wipe and respond
        mapLock.writeLock().lock();
        map.clear();
        RequestCacheValue res = scaf.setResponseType(WIPEOUT).build();
        sendResponse(res.generatePacket());
        mapLock.writeLock().unlock();

        System.gc();

        return res;
    }


    // Custom Exceptions

    class InvalidChecksumException extends Exception {}
    class KeyLengthException extends Exception {}
    class ValueLengthException extends Exception {}

}
