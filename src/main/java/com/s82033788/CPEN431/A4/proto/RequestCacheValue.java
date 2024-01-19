package com.s82033788.CPEN431.A4.proto;

import com.google.protobuf.ByteString;
import com.s82033788.CPEN431.A4.KVServerTaskHandler;
import com.s82033788.CPEN431.A4.ValueWrapper;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.zip.CRC32;

public class RequestCacheValue {
    public DatagramPacket result;
    private ResponseType responseType;
    private int errCode;
    private ValueWrapper value;
    private long pid;
    private int version;
    private int overloadWaitTime;
    private int membershipCount;
    private ByteString reqID;
    private long incomingCRC;
    private InetAddress address;
    private int port;

    private RequestCacheValue(Builder builder) {
        ResponseType type = builder.b_type;
        if(type == null) throw new IllegalArgumentException();

        this.responseType = type;

        this.address = builder.b_address;
        this.port = builder.b_port;
        this.reqID = builder.b_reqID;
        this.incomingCRC = builder.b_incomingCRC;

        switch (type) {
            case INVALID_KEY:       this.errCode = KVServerTaskHandler.RES_CODE_INVALID_KEY;        break;
            case INVALID_VALUE:     this.errCode = KVServerTaskHandler.RES_CODE_INVALID_VALUE;      break;
            case INVALID_OPCODE:    this.errCode = KVServerTaskHandler.RES_CODE_INVALID_OPCODE;     break;
            case ISALIVE:           this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case SHUTDOWN:          this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case PUT:               this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case DEL:               this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case WIPEOUT:           this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
            case INVALID_OPTIONAL:  this.errCode = KVServerTaskHandler.RES_CODE_INVALID_OPTIONAL;   break;
            case RETRY_NOT_EQUAL:   this.errCode = KVServerTaskHandler.RES_CODE_RETRY_NOT_EQUAL;    break;
            case NO_KEY:            this.errCode = KVServerTaskHandler.RES_CODE_NO_KEY;             break;
            case NO_MEM:            this.errCode = KVServerTaskHandler.RES_CODE_NO_MEM;             break;
            case OVERLOAD_CACHE:
            {
                this.errCode = KVServerTaskHandler.RES_CODE_OVERLOAD;
                this.overloadWaitTime = KVServerTaskHandler.CACHE_OVL_WAIT_TIME;
                break;
            }
            case OVERLOAD_THREAD:
            {
                this.errCode = KVServerTaskHandler.RES_CODE_OVERLOAD;
                this.overloadWaitTime = KVServerTaskHandler.THREAD_OVL_WAIT_TIME;
                break;
            }
            //need to check arguments
            case MEMBERSHIP_COUNT:
            {
                if(!builder.b_membershipCount_set) throw new IllegalArgumentException();

                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.membershipCount = builder.b_membershipCount;
                break;
            }
            case PID:
            {
                if(!builder.b_pid_set) throw new IllegalArgumentException();

                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.pid = builder.b_pid;
                break;
            }
            case VALUE:
            {
                if (builder.b_value == null) throw new IllegalArgumentException();


                this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
                this.value = builder.b_value;
                break;
            }
            default: throw new IllegalArgumentException();
        }
    }

    /* Constructors for each type of reply */
    public static class Builder {
        //mandatory params
        private ResponseType b_type;
        //optionals
        private int b_membershipCount;
        private boolean b_membershipCount_set = false;
        private long b_pid;
        private boolean b_pid_set = false;
        private ValueWrapper b_value;
        private long b_incomingCRC;
        private InetAddress b_address;
        private int b_port;
        private ByteString b_reqID;

        public Builder(long incomingCRC, InetAddress adr, int port, ByteString req_id) {
            this.b_incomingCRC = incomingCRC;
            this.b_address = adr;
            this.b_port = port;
            this.b_reqID = req_id;
        }

        public Builder setResponseType(ResponseType type) {
            if (b_type != null) throw new IllegalStateException("Type has been set already");
            this.b_type = type;
            return this;
        }

        public Builder setMembershipCount (int count) {
            if (b_membershipCount_set) throw new IllegalStateException("Membership has been set already");
            this.b_membershipCount = count;
            this.b_membershipCount_set = true;
            return this;
        }

        public Builder setPID (long pid) {
            if (b_pid_set) throw new IllegalStateException("PID set already");
            this.b_pid = pid;
            this.b_pid_set = true;
            return this;
        }

        public Builder setValue(ValueWrapper value) {
            if(b_value != null) throw new IllegalStateException("Value set already");
            this.b_value = value;
            return this;
        }

        public RequestCacheValue build() {
            return new RequestCacheValue(this);
        }


    }



//    public RequestCacheValue(ResponseType type) {
//        switch (type) {
//            case INVALID_KEY:       this.errCode = KVServerTaskHandler.RES_CODE_INVALID_KEY;        break;
//            case INVALID_VALUE:     this.errCode = KVServerTaskHandler.RES_CODE_INVALID_VALUE;      break;
//            case INVALID_OPCODE:    this.errCode = KVServerTaskHandler.RES_CODE_INVALID_OPCODE;     break;
//            case ISALIVE:           this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
//            case SHUTDOWN:          this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
//            case PUT:               this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
//            case DEL:               this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
//            case WIPEOUT:           this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;            break;
//            case INVALID_OPTIONAL:  this.errCode = KVServerTaskHandler.RES_CODE_INVALID_OPTIONAL;   break;
//            case RETRY_NOT_EQUAL:   this.errCode = KVServerTaskHandler.RES_CODE_RETRY_NOT_EQUAL;    break;
//            case NO_KEY:            this.errCode = KVServerTaskHandler.RES_CODE_NO_KEY;             break;
//            case NO_MEM:            this.errCode = KVServerTaskHandler.RES_CODE_NO_MEM;             break;
//            case OVERLOAD_CACHE:
//            {
//                this.errCode = KVServerTaskHandler.RES_CODE_OVERLOAD;
//                this.overloadWaitTime = KVServerTaskHandler.CACHE_OVL_WAIT_TIME;
//                break;
//            }
//            case OVERLOAD_THREAD:
//            {
//                this.errCode = KVServerTaskHandler.RES_CODE_OVERLOAD;
//                this.overloadWaitTime = KVServerTaskHandler.THREAD_OVL_WAIT_TIME;
//                break;
//            }
//            default: throw new IllegalArgumentException();
//        }
//        this.responseType = type;
//    }


//    public RequestCacheValue(ResponseType type, int membershipCount)
//    {
//        if(type != ResponseType.MEMBERSHIP_COUNT) throw new IllegalArgumentException();
//
//        this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
//        this.membershipCount = membershipCount;
//        this.responseType = type;
//    }

//    public RequestCacheValue(ResponseType type, long pid)
//    {
//        if (type != ResponseType.PID) throw new IllegalArgumentException();
//
//        this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
//        this.pid = pid;
//        this.responseType = type;
//    }

//    public RequestCacheValue(ResponseType type, ValueWrapper value) {
//        if(type != ResponseType.VALUE) throw new IllegalArgumentException();
//
//        this.errCode = KVServerTaskHandler.RES_CODE_SUCCESS;
//        this.value = value;
//        this.version = value.getVersion();
//        this.responseType = type;
//    }

    public ByteString generatePayload()
    {
        switch (this.responseType) {
            case INVALID_KEY:
            case INVALID_VALUE:
            case INVALID_OPCODE:
            case ISALIVE:
            case SHUTDOWN:
            case PUT:
            case DEL:
            case WIPEOUT:
            case INVALID_OPTIONAL:
            case RETRY_NOT_EQUAL:
            case NO_KEY:
            case NO_MEM:
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(errCode)
                        .build()
                        .toByteString();
            case OVERLOAD_THREAD:
            case OVERLOAD_CACHE:
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(errCode)
                        .setOverloadWaitTime(overloadWaitTime)
                        .build()
                        .toByteString();
            case MEMBERSHIP_COUNT:
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(errCode)
                        .setMembershipCount(membershipCount)
                        .build().toByteString();
            case PID:
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(errCode)
                        .setPid((int) pid)
                        .build()
                        .toByteString();
            case VALUE:
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(errCode)
                        .setValue(ByteString.copyFrom(value.getValue()))
                        .setVersion(value.getVersion())
                        .build()
                        .toByteString();
            default: throw new IllegalStateException();
        }

    }

    public DatagramPacket generatePacket() {
        //prepare checksum
        byte[] fullBody = reqID.concat(generatePayload()).toByteArray();
        CRC32 crc32 = new CRC32();
        crc32.update(fullBody);
        long msgChecksum = crc32.getValue();

        //prepare message
        byte[] msg = Message.Msg.newBuilder()
                .setMessageID(reqID)
                .setPayload(this.generatePayload())
                .setCheckSum(msgChecksum)
                .build()
                .toByteArray();

        DatagramPacket pkt = new DatagramPacket(msg, msg.length, address, port);
        return pkt;
    }
//    public RequestCacheValue(DatagramPacket result, long incomingCRC) {
//        this.result = result;
//        this.incomingCRC = incomingCRC;
//    }

    public int getErrCode() {
        return errCode;
    }

    public ValueWrapper getValue() {
        return value;
    }

    public long getPid() {
        return pid;
    }

    public int getVersion() {
        return version;
    }

    public int getOverloadWaitTime() {
        return overloadWaitTime;
    }

    public int getMembershipCount() {
        return membershipCount;
    }

    public long getIncomingCRC() {
        return incomingCRC;
    }
}

