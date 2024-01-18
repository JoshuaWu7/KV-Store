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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.zip.CRC32;

public class KVServerTaskHandler implements Runnable {
    DatagramPacket iPacket;
    DatagramSocket socket;
    Cache<RequestCacheKey, RequestCacheValue> requestCache;
    ConcurrentMap<KeyWrapper, ValueWrapper> map;
    ReadWriteLock mapLock;
    DatagramPacket outgoingResponse;
    final static int KEY_MAX_LEN = 32;
    final static int VALUE_MAX_LEN = 10_000;

    public KVServerTaskHandler(DatagramPacket iPacket,
                               DatagramSocket socket,
                               Cache<RequestCacheKey, RequestCacheValue> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map, ReadWriteLock mapLock) {
        this.iPacket = iPacket;
        this.socket = socket;
        this.requestCache = requestCache;
        this.map = map;
        this.mapLock = mapLock;
    }


    @Override
    public void run() {
        //TODO perform memory and cache runtime checks

        //cache might have data race, be alert.
        if (outgoingResponse != null) throw new IllegalStateException();

        //decode the message
        UnwrappedMessage unwrappedMessage;
        try {
            unwrappedMessage = unpackPacket(iPacket);
        } catch (InvalidProtocolBufferException e) {
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

       //TODO check cache and return early if hit
        RequestCacheValue reply;
        try {
            reply = requestCache.get(new RequestCacheKey(unwrappedMessage.reqID.toByteArray(), unwrappedMessage.crc),
                    () -> newProcessRequest(unwrappedMessage));
        } catch (ExecutionException e) {
            //TODO deal with this
            throw new RuntimeException(e);
        }

        //assert that the request is exactly the same
        if(reply.incomingCRC != unwrappedMessage.crc) {
            try {
                replyMsg(unwrappedMessage.reqID, replyRetryNotEqual());
            } catch (IOException e) {
                System.err.println("Unable to send mismatched retry due to IO");
                System.err.println("Will not send response packet");
            }
            return;
        }

        //send the reply, no concurrency problem here, since packet will be sent eventually.
        // (tbh don't even need to synchronize load other than to avoid double loading)
        if (outgoingResponse == null) {
            try {
                outgoingResponse = reply.result;
                socket.send(reply.result);
            } catch (IOException e) {
                System.err.println("Unable to send cached response due to IO");
                System.err.println("Will not send response packet");
            }
            return;
        }
    }

    private RequestCacheValue newProcessRequest(UnwrappedMessage unwrappedMessage) throws
            IOException {


        UnwrappedPayload payload;

        try {
            payload = unpackPayload(unwrappedMessage.payload);
        }  catch (ValueLengthException e) {
            System.err.println("Value field exceeds size");
            System.err.println("Sending rejection packet");

            DatagramPacket responsePacket;
            responsePacket = replyMsg(unwrappedMessage.reqID, replyInvalidValuePayload());
            return new RequestCacheValue(responsePacket, unwrappedMessage.crc);

        } catch (KeyLengthException e) {
            System.err.println("Value field exceeds size");
            System.err.println("Sending rejection packet");

            DatagramPacket responsePacket;
            responsePacket = replyMsg(unwrappedMessage.reqID, replyInvalidValuePayload());
            return new RequestCacheValue(responsePacket, unwrappedMessage.crc);
        }

        //process the packet by request code
        ByteString reqID = unwrappedMessage.reqID;
        switch(payload.command)
        {
            case 0x01: handlePut(reqID, payload); break;
            case 0x02: handleGet(reqID, payload); break;
            case 0x03: handleDelete(reqID, payload); break;
            case 0x04: handleShutdown(reqID, payload); break;
            case 0x05: handleWipeout(reqID, payload);  break;
            case 0x06: handleIsAlive(reqID, payload); break;
            case 0x07: handleGetPID(reqID, payload); break;
            case 0x08: handleGetMembershipCount(reqID, payload);  break;

            default: replyMsg(reqID, replyInvalidOpcode());
        }

        if(outgoingResponse == null) throw new IllegalStateException();
        return new RequestCacheValue(outgoingResponse, unwrappedMessage.crc);
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


    //helper function to send a packet given payload and id. Mutates outgoingResponse
    private DatagramPacket replyMsg(ByteString id, ByteString payload) throws IOException {
        //check that response to this incoming packet has not been sent
        if(outgoingResponse != null) throw new IllegalStateException();

        //prepare checksum
        byte[] fullBody = id.concat(payload).toByteArray();
        CRC32 crc32 = new CRC32();
        crc32.update(fullBody);
        long msgChecksum = crc32.getValue();

        //prepare message
        byte[] msg = Message.Msg.newBuilder()
                    .setMessageID(id)
                    .setPayload(payload)
                    .setCheckSum(msgChecksum)
                    .build()
                    .toByteArray();

        DatagramPacket pkt = new DatagramPacket(msg, msg.length, iPacket.getAddress(), iPacket.getPort());
        socket.send(pkt);
        outgoingResponse = pkt;
        return pkt;
    }

    //Helper functions to generate payloads
    private ByteString replyInvalidKeyPayload() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(6)
                .build()
                .toByteString();
    }

    private ByteString replyInvalidValuePayload() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(7)
                .build()
                .toByteString();
    }

    private ByteString replyInvalidOpcode() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(5)
                .build()
                .toByteString();
    }

    private ByteString replyMembershipCount(int count)
    {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x0)
                .setMembershipCount(count)
                .build()
                .toByteString();
    }

    private ByteString replyPID(long pid) {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x0)
                .setPid(Math.toIntExact(pid))
                .build()
                .toByteString();
    }

    private ByteString replyIsAlive() {
        return replySuccessOnly();
    }

    private ByteString replySuccessOnly() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x0)
                .build()
                .toByteString();
    }

    private ByteString replyInvalidOptionalVal() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x21)
                .build()
                .toByteString();
    }

    private ByteString replyRetryNotEqual() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x22)
                .build()
                .toByteString();
    }

    private ByteString replyValue(ValueWrapper value){
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x0)
                .setValue(ByteString.copyFrom(value.value))
                .setVersion(value.version)
                .build()
                .toByteString();
    }

    private ByteString replyNoSuchKey() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(0x01)
                .build()
                .toByteString();
    }

    //helper functions to process requests
    private void handleGetMembershipCount(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        replyMsg(reqID, replyMembershipCount(1));
    }

    private void handleGetPID(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        long pid = ProcessHandle.current().pid();
        replyMsg(reqID, replyPID(pid));
    }

    private void handleIsAlive(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        replyMsg(reqID, replyIsAlive());
    }

    private void handleShutdown(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }


        replyMsg(reqID, replySuccessOnly());
        System.out.println("Recevied shutdown command, shutting down now");
        System.exit(0);
    }

    private void handlePut(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if(!payload.keyExists || !payload.valueExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        // no need to check version, since by default it is parsed as 0,
        // which according to the spec, is the default value we want

        //defensive design to reject 0 length keys
        if(payload.key.length <= 0)
        {
            replyMsg(reqID, replyInvalidKeyPayload());
            return;
        }

        //atomically put and respond, `tis thread safe.
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception= new AtomicReference<>();
        map.compute(new KeyWrapper(payload.key), (key, value) -> {
            try {
                replyMsg(reqID, replySuccessOnly());
                return new ValueWrapper(payload.value, payload.version);
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
    }

    private void handleGet(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if((!payload.keyExists) || payload.valueExists || payload.versionExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        //defensive design to reject 0 length keys
        if(payload.key.length <= 0)
        {
            replyMsg(reqID, replyInvalidKeyPayload());
            return;
        }

        //atomically get and respond
        mapLock.readLock().lock();
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.key), (key, value) -> {
            try {
                if (value == null) {
                    replyMsg(reqID, replyNoSuchKey());
                    return value;
                } else {
                    replyMsg(reqID, replyValue(value));
                    return value;
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

    }

    private void handleDelete(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if((!payload.keyExists) || payload.valueExists || payload.versionExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        //defensive design to reject 0 length keys
        if(payload.key.length <= 0)
        {
            replyMsg(reqID, replyInvalidKeyPayload());
            return;
        }

        //atomically del and respond
        AtomicReference<IOException> ioexception = new AtomicReference<>();
        mapLock.readLock().lock();
        map.compute(new KeyWrapper(payload.key), (key, value) -> {
            try {
                if (value == null) {
                    replyMsg(reqID, replyNoSuchKey());
                    return value;
                } else {
                    replyMsg(reqID, replySuccessOnly());
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

    }

    private void handleWipeout(ByteString reqID, UnwrappedPayload payload) throws IOException {
        if(payload.valueExists || payload.versionExists || payload.keyExists)
        {
            replyMsg(reqID, replyInvalidOptionalVal());
            return;
        }

        //atomically wipe and respond
        mapLock.writeLock().lock();
        map.clear();
        replyMsg(reqID, replySuccessOnly());
        mapLock.writeLock().unlock();
    }


    // Custom Exceptions

    class InvalidChecksumException extends Exception {}
    class KeyLengthException extends Exception {}
    class ValueLengthException extends Exception {}

}
