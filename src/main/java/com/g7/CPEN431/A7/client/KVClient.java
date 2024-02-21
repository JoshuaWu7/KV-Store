package com.g7.CPEN431.A7.client;


import com.g7.CPEN431.A7.newProto.KVMsg.KVMsgSerializer;
import com.g7.CPEN431.A7.newProto.KVRequest.KVRequestSerializer;
import com.g7.CPEN431.A7.newProto.KVRequest.ServerEntry;
import com.g7.CPEN431.A7.newProto.KVResponse.KVResponseSerializer;
import com.g7.CPEN431.A7.newProto.KVResponse.ServerResponseFactory;
import com.g7.CPEN431.A7.wrappers.UnwrappedMessage;
import com.g7.CPEN431.A7.wrappers.UnwrappedPayload;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

public class KVClient {
    private InetAddress serverAddress;
    private int serverPort;
    private DatagramSocket socket;
    byte[] publicBuf;
    int testSequence;
    UnwrappedMessage messageOnWire;

    /* Test Result codes */
    public final static int TEST_FAILED = 0;
    public final static int TEST_PASSED = 1;
    public final static int TEST_UNDECIDED = 2;

    /* Request Codes */
    public final static int REQ_CODE_PUT = 0x01;
    public final static int REQ_CODE_GET = 0X02;
    public final static int REQ_CODE_DEL = 0X03;
    public final static int REQ_CODE_SHU = 0X04;
    public final static int REQ_CODE_WIP = 0X05;
    public final static int REQ_CODE_ALI = 0X06;
    public final static int REQ_CODE_PID = 0X07;
    public final static int REQ_CODE_MEM = 0X08;
    public final static int REQ_CODE_DED = 0x100;

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

    public KVClient(InetAddress serverAddress, int serverPort, DatagramSocket socket, byte[] publicBuf) throws SocketException {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = socket;
        this.publicBuf = publicBuf;

        socket.setSoTimeout(100);
    }

    public KVClient(InetAddress serverAddress, int serverPort, DatagramSocket socket, byte[] publicBuf, int testSequence) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = socket;
        this.publicBuf = publicBuf;
        this.testSequence = testSequence;
    }

    public void setDestination(InetAddress serverAddress, int serverPort)
    {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }
    public int administrationTest() throws IOException {
        System.out.println("Administration Test. PID -> ALI -> MEM -> SHU -> ALI");

        /* Generate some random values for key, value, and version */

        ServerResponse p;
        System.out.println("Getting PID");
        try {
            p = getPID();
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PID failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Checking server is alive");
        ServerResponse a;
        try {
            a = isAlive();
        } catch (Exception e) {
            return handleException(e);
        }

        if(a.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("ALI with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Checking Membership Count");
        ServerResponse m;
        try {
            m = getMembershipCount();
        } catch (Exception e) {
            return handleException(e);
        }

        if(m.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("MEM with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        if(m.getMembershipCount() != 1) {
            System.out.println("MEM count mismatch. Count:" + p.getMembershipCount());
            return TEST_FAILED;
        }

        try {
            shutdown();
        } catch (ServerTimedOutException e) {
            System.out.println("Server shut down");
        } catch (Exception e) {
            return handleException(e);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.out.println("Sleep interrupted");
            return TEST_UNDECIDED;
        }

        //Test isAlive
        try {
            isAlive();
        } catch (ServerTimedOutException e) {
            System.out.println("Server is not alive after shutdown");
            System.out.println("***PASSED***");
            return TEST_PASSED;
        } catch (Exception e) {
            return handleException(e);
        }

        System.out.println("Server is still alive");
        return TEST_FAILED;
    }


   /* Helper functions corresponding to each type of request */
    private void shutdown() throws IOException, InterruptedException, ServerTimedOutException, MissingValuesException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_SHU);

        System.out.println("Sleeping for 5s");
        Thread.sleep(5000);


        sendAndReceiveServerResponse(pl);
    }
    private ServerResponse wipeout() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_WIP);


        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse delete(byte[] key) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_DEL);
        pl.setKey(key);


        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse get(byte[] key) throws IOException, MissingValuesException, ServerTimedOutException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_GET);
        pl.setKey(key);


        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasValue())
            throw new MissingValuesException("Value");

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasVersion())
            throw new MissingValuesException("Version");

        return res;
    }
    private ServerResponse put(byte[] key, byte[] value, int version) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PUT);
        pl.setKey(key);
        pl.setValue(value);
        pl.setVersion(version);

        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse getMembershipCount() throws IOException, MissingValuesException, ServerTimedOutException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_MEM);

        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasMembershipCount())
            throw new MissingValuesException("Membership Count");

        return res;
    }
    private ServerResponse getPID() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PID);

        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasPid())
            throw new MissingValuesException("PID");

        return res;
    }
    private ServerResponse isAlive() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_ALI);

        return sendAndReceiveServerResponse(pl);
    }

    public ServerResponse isDead(List<ServerEntry> l) throws MissingValuesException, IOException, ServerTimedOutException, InterruptedException {
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_DED);
        pl.setServerRecord(l);

        return sendAndReceiveServerResponse(pl);
    }

    /* Utilities to generate and process outgoing and incoming packets following the retry and At most once policy */
    private ServerResponse sendAndReceiveServerResponse(UnwrappedPayload pl) throws
            IOException,
            ServerTimedOutException,
            InterruptedException,
            MissingValuesException {
        ServerResponse res;

        UnwrappedMessage msg = generateMessage(pl);
        res = sendAndReceiveSingleServerResponse(msg);

       /* Send a new packet after overload time*/
        while(res.getErrCode() == RES_CODE_OVERLOAD){
            if(!res.hasOverloadWaitTime()) throw new MissingValuesException("Overload wait time");
            Thread.sleep(res.getOverloadWaitTime());

            msg = generateMessage(pl);
            res = sendAndReceiveSingleServerResponse(msg);
        }

        return res;
    }

     byte[] generateMsgID() throws UnknownHostException {
        byte[] rand = new byte[2];
        try {
            SecureRandom.getInstanceStrong().nextBytes(rand);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer idAndPl = ByteBuffer.allocate(16);
        idAndPl.order(ByteOrder.LITTLE_ENDIAN);
        idAndPl.put(Inet4Address.getLocalHost().getAddress());
        idAndPl.putShort((short) (socket.getPort() - 32768));
        idAndPl.put(rand);
        idAndPl.putLong(System.nanoTime());

        return idAndPl.array();
    }

    UnwrappedMessage generateMessage(UnwrappedPayload pl) throws UnknownHostException {
        byte[] plb = KVRequestSerializer.serialize(pl);

        ByteBuffer idAndpl = ByteBuffer.allocate(16 + plb.length);

        byte[] msgID = generateMsgID();

        idAndpl.put(msgID);
        idAndpl.put(plb);
        idAndpl.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(idAndpl.array());
        long checksum = crc32.getValue();

        UnwrappedMessage msg = new UnwrappedMessage();
        msg.setCheckSum(checksum);
        msg.setPayload(plb);
        msg.setMessageID(msgID);
        return msg;
    }

    ServerResponse sendAndReceiveSingleServerResponse(UnwrappedMessage req) throws ServerTimedOutException, IOException {
        DatagramPacket rP = new DatagramPacket(publicBuf, publicBuf.length);

        int tries = 0;
        boolean success = false;

        byte[] msgb = KVMsgSerializer.serialize(req);
        DatagramPacket p = new DatagramPacket(msgb, msgb.length, serverAddress, serverPort);
        int initTimeout = socket.getSoTimeout();

        messageOnWire = req;

        UnwrappedMessage res = null;
        while (tries < 4 && !success)
        {
            socket.send(p);
            try {
                socket.receive(rP);
            } catch (SocketTimeoutException e) {
                System.out.println("Timed out");
                tries++;
                continue;
                //do nothing and let it loop
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            //verify message ID and CRC
            byte[] trimmed = Arrays.copyOf(rP.getData(), rP.getLength());
            res = (UnwrappedMessage) KVMsgSerializer.parseFrom(new ServerResponseFactory(), trimmed);

            boolean msgIDMatch = res.hasMessageID() && Arrays.equals(res.getMessageID(), req.getMessageID());

            ByteBuffer rIDnPL = ByteBuffer.allocate(res.getMessageID().length + res.getPayload().length);
            rIDnPL.put(res.getMessageID());
            rIDnPL.put(res.getPayload());
            rIDnPL.flip();

            CRC32 crc32 = new CRC32();
            crc32.update(rIDnPL.array());
            boolean crc32Match = crc32.getValue() == res.getCheckSum();

            success = msgIDMatch && crc32Match;
            tries++;
            socket.setSoTimeout(socket.getSoTimeout() * 2);
        }

        socket.setSoTimeout(initTimeout);

        if(tries == 3 && !success) {
            System.out.println("Did not receive response after 3 tries");
            throw new ServerTimedOutException();
        }

        ServerResponse plr = (ServerResponse) KVResponseSerializer.parseFrom(new ServerResponseFactory(), res.getPayload());

        return plr;
    }

    /* Exception Handler */
    int handleException(Exception e) {
        if (e instanceof ServerTimedOutException) {
            System.out.println("Server timed out. Aborting");
            return TEST_UNDECIDED;
        } else if (e instanceof  MissingValuesException) {
            System.out.println("Server response did not contain value: " + ((MissingValuesException)e).missingItem);
            return TEST_FAILED;
        } else if (e instanceof InterruptedException) {
            System.out.println("Thread interrupted.");
            return TEST_UNDECIDED;
        } else if (e instanceof  IOException)
        {
            System.out.println("Error in parsing.");
            return TEST_FAILED;
        }
        else {
            e.printStackTrace();
            return TEST_UNDECIDED;
        }

    }

    /* Custom exceptions*/
    class ServerTimedOutException extends Exception{}
    class MissingValuesException extends Exception{
        public String missingItem;

        public MissingValuesException(String missingItem) {
            this.missingItem = missingItem;
        }
    }

}