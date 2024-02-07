package com.s82033788.CPEN431.A4;

import com.google.common.cache.Cache;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import static com.s82033788.CPEN431.A4.cache.ResponseType.*;

public class KVServerTaskHandler implements Runnable {
    /* Thread parameters */
    private final AtomicInteger bytesUsed;
    private final DatagramPacket iPacket;
    private final Cache<RequestCacheKey, DatagramPacket> requestCache;
    private final ConcurrentMap<KeyWrapper, ValueWrapper> map;
    private final ReadWriteLock mapLock;
    private boolean responseSent = false;
    private PublicBuffer incomingPublicBuffer;

    final private ConcurrentLinkedQueue<byte[]> bytePool;  //this is thread safe
    final private boolean isOverloaded;
    final private ConcurrentLinkedQueue<DatagramPacket> outbound;

    /* Constants */
    final static int KEY_MAX_LEN = 32;
    final static int VALUE_MAX_LEN = 10_000;
    public final static int CACHE_OVL_WAIT_TIME = 80;   // Temporarily unused since cache doesn't overflow
    public final static int THREAD_OVL_WAIT_TIME = 16;
    public final static int MAP_SZ = 60_817_408;

    /* Response Codes */
    public final static int RES_CODE_SUCCESS = 0x0;
    public final static int RES_CODE_NO_KEY = 0x1;
    public final static int RES_CODE_NO_MEM = 0x2;
    public final static int RES_CODE_OVERLOAD = 0x3;
    public final static int RES_CODE_INTERNAL_ER = 0x4;
    public final static int RES_CODE_INVALID_OPCODE = 0x5;
    public final static int RES_CODE_INVALID_KEY = 0x6;
    public final static int RES_CODE_INVALID_VALUE = 0x7;
    public final static int RES_CODE_INVALID_OPTIONAL = 0x21;
    public final static int RES_CODE_RETRY_NOT_EQUAL = 0x22;

    /* Request Codes */
    public final static int REQ_CODE_PUT = 0x01;
    public final static int REQ_CODE_GET = 0X02;
    public final static int REQ_CODE_DEL = 0X03;
    public final static int REQ_CODE_SHU = 0X04;
    public final static int REQ_CODE_WIP = 0X05;
    public final static int REQ_CODE_ALI = 0X06;
    public final static int REQ_CODE_PID = 0X07;
    public final static int REQ_CODE_MEM = 0X08;

    public KVServerTaskHandler(DatagramPacket iPacket,
                               Cache<RequestCacheKey, DatagramPacket> requestCache,
                               ConcurrentMap<KeyWrapper, ValueWrapper> map,
                               ReadWriteLock mapLock,
                               AtomicInteger bytesUsed,
                               ConcurrentLinkedQueue<byte[]> bytePool,
                               boolean isOverloaded,
                               ConcurrentLinkedQueue<DatagramPacket> outbound) {
        this.iPacket = iPacket;
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
            throw e;
        }
        finally {
            // Return shared objects to the pool
            bytePool.offer(iPacket.getData());
        }
    }

    /**
     * Executes the main logic of the thread to process incoming requests and replies.
     */
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


        DatagramPacket reply;
        try {
            reply = requestCache.get(new RequestCacheKey(unwrappedMessage.getMessageID(), unwrappedMessage.getCheckSum()),
                    () -> newProcessRequest(unwrappedMessage));
        } catch (ExecutionException e) {
            if(e.getCause() instanceof IOException)
            {
                System.err.println("Unable to decode payload. Doing nothing");
            }
            else {
                System.err.println(e);
            }
            return;
        }

        // Send the packet (if it was in cache, and the cache loading function was not called)
        // (tbh don't even need to synchronize load other than to avoid double loading)
        if (!responseSent) {
            sendResponse(reply);
        }
    }

    /**
     * The function that handles the request and returns responses after the message is unwrapped
     * @param unwrappedMessage The unwrapped message
     * @return The packet sent in response
     * @throws IOException If there are problems unpacking or packing into the public buffer
     */
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

        //process the packet by request code
        DatagramPacket res;
        switch(payload.getCommand())
        {
            case REQ_CODE_PUT: res = handlePut(scaf, payload); break;
            case REQ_CODE_GET: res = handleGet(scaf, payload); break;
            case REQ_CODE_DEL: res = handleDelete(scaf, payload); break;
            case REQ_CODE_SHU: res = handleShutdown(scaf, payload); break;
            case REQ_CODE_WIP: res = handleWipeout(scaf, payload);  break;
            case REQ_CODE_ALI: res = handleIsAlive(scaf, payload); break;
            case REQ_CODE_PID: res =  handleGetPID(scaf, payload); break;
            case REQ_CODE_MEM: res = handleGetMembershipCount(scaf, payload);  break;

            default: {
                RequestCacheValue val = scaf.setResponseType(INVALID_OPCODE).build();
                res =  generateAndSend(val);
            }
        }

        //response should have been sent out by one of the above functions, so this is just extra checking.
        if(!responseSent) {
            System.out.println(payload.getCommand());
            throw new IllegalStateException();
        }
        return res;
    }

    /**
     * Helper function to unpack packet
     * @param iPacket incoming packet with client request
     * @return And unwrapped packet
     * @throws IOException If there was a problem parsing the content into the public buffer
     * @throws InvalidChecksumException If the checksum does not match
     */
    private UnwrappedMessage unpackPacket(DatagramPacket iPacket)
            throws IOException, InvalidChecksumException {

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

    /**
     * unpacks the payload into an accesible format
     * @param payload
     * @return The unpacked object
     * @throws IOException If there was a problem unpacking it from the public buffer;
     */
    private UnwrappedPayload unpackPayload(PublicBuffer payload) throws
            IOException{

        KVRequest deserialized = KVRequestSerializer.parseFrom(new KVRequestFactory(), payload.readPayloadFromPBBody());
        return (UnwrappedPayload) deserialized;
    }

    /**
     * Enqueues the packet to be sent by sender thread;
     * @param d Packet to send
     */
    private void sendResponse(DatagramPacket d)  {
        if (responseSent) throw new IllegalStateException();

        responseSent = true;
        outbound.offer(d);
    }


    /**
     * @param res Prebuilt body to be sent
     * @return the packet to be sent.
     */
    DatagramPacket generateAndSend(RequestCacheValue res) {
        DatagramPacket pkt = res.generatePacket();
        sendResponse(pkt);
        return pkt;
    }
    //helper functions to process requests

    /**
     *  Sends the response if the request is for membership count
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleGetMembershipCount(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
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

    /**
     * Helper function that responds to GET PID Requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleGetPID(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
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


    /**
     * Helper function that responds to is Alive Requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleIsAlive(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        RequestCacheValue res = scaf.setResponseType(ISALIVE).build();
        return generateAndSend(res);
    }


    /**
     * Helper function that responds to shutdown requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
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

    /**
     * Helper function that responds to put requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handlePut(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if(!payload.hasKey() || !payload.hasValue())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length == 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        if(payload.getValue().length > VALUE_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_VALUE).build();
            return generateAndSend(res);
        }


        if(bytesUsed.get() >= MAP_SZ) {
            RequestCacheValue res = scaf.setResponseType(NO_MEM).build();
            return generateAndSend(res);
        }

        mapLock.readLock().lock();

        //atomically put and respond, `tis thread safe.
        AtomicReference<IOException> ioexception= new AtomicReference<>();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            RequestCacheValue res = scaf.setResponseType(PUT).build();
            pkt.set(generateAndSend(res));
            bytesUsed.addAndGet(payload.getValue().length);
            return new ValueWrapper(payload.getValue(), payload.getVersion());
        });
        mapLock.readLock().unlock();

        return pkt.get();
    }

    /**
     * Helper function that responds to get requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleGet(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if((!payload.hasKey()) || payload.hasValue() || payload.hasVersion())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length == 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        //atomically get and respond
        mapLock.readLock().lock();
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
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
        });
        mapLock.readLock().unlock();

        return pkt.get();
    }

    /**
     * Helper function that responds delete requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleDelete(RequestCacheValue.Builder scaf, UnwrappedPayload payload) {
        if((!payload.hasKey()) || payload.hasValue() || payload.hasVersion())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //defensive design to reject 0 length keys
        if(payload.getKey().length == 0 || payload.getKey().length > KEY_MAX_LEN)
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_KEY).build();
            return generateAndSend(res);
        }

        //atomically del and respond
        AtomicReference<DatagramPacket> pkt = new AtomicReference<>();
        mapLock.readLock().lock();
        map.compute(new KeyWrapper(payload.getKey()), (key, value) -> {
            if (value == null) {
                RequestCacheValue res = scaf.setResponseType(NO_KEY).build();
                pkt.set(generateAndSend(res));
            } else {
                bytesUsed.addAndGet(-value.getValue().length);
                RequestCacheValue res = scaf.setResponseType(DEL).build();
                pkt.set(generateAndSend(res));
            }
            return null;
        });
        mapLock.readLock().unlock();

        return pkt.get();
    }

    /**
     * Helper function that responds to wipeout requests
     * @param scaf Scaffold builder for Request CacheValue that is partially filled in (with IP / port etc.)
     * @param payload Payload from the client
     * @return The packet sent
     */
    private DatagramPacket handleWipeout(RequestCacheValue.Builder scaf, UnwrappedPayload payload){
        if(payload.hasValue() || payload.hasVersion() || payload.hasKey())
        {
            RequestCacheValue res = scaf.setResponseType(INVALID_OPTIONAL).build();
            return generateAndSend(res);
        }

        //atomically wipe and respond
        mapLock.writeLock().lock();
        map.clear();
        bytesUsed.set(0); //concurrently correct, because we are only thread with access to map
        RequestCacheValue res = scaf.setResponseType(WIPEOUT).build();
        DatagramPacket pkt = generateAndSend(res);
        mapLock.writeLock().unlock();

        System.gc();

        return pkt;
    }

    // Custom Exceptions

    static class InvalidChecksumException extends Exception {}
}
