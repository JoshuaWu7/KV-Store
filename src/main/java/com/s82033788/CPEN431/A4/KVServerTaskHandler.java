package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.s82033788.CPEN431.A4.proto.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.zip.CRC32;

public class KVServerTaskHandler implements Runnable {
    DatagramPacket iPacket;
    DatagramSocket socket;
    Cache<RequestCacheKey, RequestCacheValue> requestCache;
    final static int KEY_MAX_LEN = 32;
    final static int VALUE_MAX_LEN = 10_000;

    public KVServerTaskHandler(DatagramPacket iPacket, DatagramSocket socket, Cache<RequestCacheKey, RequestCacheValue> requestCache) {
        this.iPacket = iPacket;
        this.socket = socket;
        this.requestCache = requestCache;
    }

    @Override
    public void run() {
        //TODO perform memory and cache runtime checks

        //decode the message
        UnwrappedMessage unwrappedMessage;
        try {
            unwrappedMessage = unpackPacket(iPacket);
        } catch (InvalidProtocolBufferException e) {
            System.err.println("Packet does not match .proto");
            System.err.println(e);
            System.err.println("Stopping packet handling and returning");

            //TODO consider custom error code
            return;
        } catch (InvalidChecksumException e) {
            System.err.println("Packet checksum does not match");
            System.err.println("Stopping packet handling and returning");


            //TODO consider custom error code
            return;
        }

       //TODO check cache and return early if hit

        //decode the payload
        try {
            unpackPayload(unwrappedMessage.payload);
        } catch (InvalidProtocolBufferException e) {
            System.err.println("Payload does not match .proto");
            System.err.println(e);
            System.err.println("Stopping packet handling and returning");

            //TODO custom error code
            return;
        } catch (ValueLengthException e) {
            System.err.println("Value field exceeds size");
            System.err.println("Sending rejection packet");

            //TODO send rejection packet
            try {
                replyMsg(unwrappedMessage.reqID, replyInvalidValuePayload());
            } catch (IOException ex) {
                System.err.println("Socket exception");
                System.err.println(ex);
                System.err.println("Returning without sending rejection packet");
            }
            return;

        } catch (KeyLengthException e) {
            System.err.println("Value field exceeds size");
            System.err.println("Sending rejection packet");

            try {
                replyMsg(unwrappedMessage.reqID, replyInvalidKeyPayload());
            } catch (IOException ex) {
                System.err.println("Socket exception");
                System.err.println(ex);
                System.err.println("Returning without sending rejection packet");
            }
            return;
        }


    }

    private UnwrappedMessage unpackPacket(DatagramPacket iPacket)
            throws InvalidProtocolBufferException, InvalidChecksumException {
        long expectedCRC;
        ByteString payload;
        ByteString messageID;


        byte[] trimmedMsg = Arrays.copyOf(iPacket.getData(), iPacket.getLength());
        Message.Msg deserialized = Message.Msg.parseFrom(trimmedMsg);
        expectedCRC = deserialized.getCheckSum();
        payload = deserialized.getPayload();
        messageID = deserialized.getPayload();

        //verify checksum
        byte[] allBody = messageID.concat(payload).toByteArray();
        CRC32 crc32 = new CRC32();
        crc32.update(allBody);
        long actualCRC = crc32.getValue();
        if (actualCRC != expectedCRC) throw new InvalidChecksumException();

        return new UnwrappedMessage(messageID, payload, actualCRC);
    }

    private UnwrappedPayload unpackPayload(ByteString payload) throws
            InvalidProtocolBufferException, KeyLengthException, ValueLengthException {
        KeyValueRequest.KVRequest deserialized = KeyValueRequest.KVRequest.parseFrom(payload);
        int command = deserialized.getCommand();
        byte[] key = deserialized.toByteArray();
        byte[] value = deserialized.toByteArray();
        int version = deserialized.getVersion();

        if(key.length > KEY_MAX_LEN) throw new KeyLengthException();
        if(value.length > VALUE_MAX_LEN) throw new ValueLengthException();

        return new UnwrappedPayload(command, key, value, version);
    }


    private void replyMsg(ByteString id, ByteString payload) throws IOException {
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
    }
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

    class InvalidChecksumException extends Exception {}
    class KeyLengthException extends Exception {}
    class ValueLengthException extends Exception {}
}
