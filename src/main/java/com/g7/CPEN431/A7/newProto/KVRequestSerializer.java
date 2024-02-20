package com.g7.CPEN431.A7.newProto;
public final class KVRequestSerializer {
public static byte[] serialize(KVRequest message) {
try {
assertInitialized(message);
int totalSize = 0;
if (message.hasCommand()) {
totalSize += ProtobufOutputStream.computeUint32Size(1, message.getCommand());
}
if (message.hasKey()) {
totalSize += message.getKey().length;
totalSize += ProtobufOutputStream.computeTagSize(2);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getKey().length);
}
if (message.hasValue()) {
totalSize += message.getValue().length;
totalSize += ProtobufOutputStream.computeTagSize(3);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getValue().length);
}
if (message.hasVersion()) {
totalSize += ProtobufOutputStream.computeInt32Size(4, message.getVersion());
}
if (message.hasServerAddress()) {
totalSize += message.getServerAddress().length;
totalSize += ProtobufOutputStream.computeTagSize(100);
totalSize += ProtobufOutputStream.computeRawVarint32Size(message.getServerAddress().length);
}
if (message.hasServerPort()) {
totalSize += ProtobufOutputStream.computeUint32Size(101, message.getServerPort());
}
if (message.hasInformationTime()) {
totalSize += ProtobufOutputStream.computeUint64Size(102, message.getInformationTime());
}
final byte[] result = new byte[totalSize];
int position = 0;
if (message.hasCommand()) {
position = ProtobufOutputStream.writeUint32(1, message.getCommand(), result, position);
}
if (message.hasKey()) {
position = ProtobufOutputStream.writeBytes(2, message.getKey(), result, position);
}
if (message.hasValue()) {
position = ProtobufOutputStream.writeBytes(3, message.getValue(), result, position);
}
if (message.hasVersion()) {
position = ProtobufOutputStream.writeInt32(4, message.getVersion(), result, position);
}
if (message.hasServerAddress()) {
position = ProtobufOutputStream.writeBytes(100, message.getServerAddress(), result, position);
}
if (message.hasServerPort()) {
position = ProtobufOutputStream.writeUint32(101, message.getServerPort(), result, position);
}
if (message.hasInformationTime()) {
position = ProtobufOutputStream.writeUint64(102, message.getInformationTime(), result, position);
}
ProtobufOutputStream.checkNoSpaceLeft(result, position);
return result;
} catch (Exception e) {
throw new RuntimeException(e);
}
}
public static void serialize(KVRequest message, java.io.OutputStream os) {
try {
assertInitialized(message);
if (message.hasCommand()) {
ProtobufOutputStream.writeUint32(1, message.getCommand(), os);
}
if (message.hasKey()) {
ProtobufOutputStream.writeBytes(2, message.getKey(), os);
}
if (message.hasValue()) {
ProtobufOutputStream.writeBytes(3, message.getValue(), os);
}
if (message.hasVersion()) {
ProtobufOutputStream.writeInt32(4, message.getVersion(), os);
}
if (message.hasServerAddress()) {
ProtobufOutputStream.writeBytes(100, message.getServerAddress(), os);
}
if (message.hasServerPort()) {
ProtobufOutputStream.writeUint32(101, message.getServerPort(), os);
}
if (message.hasInformationTime()) {
ProtobufOutputStream.writeUint64(102, message.getInformationTime(), os);
}
} catch (java.io.IOException e) {
throw new RuntimeException("Serializing to a byte array threw an IOException (should never happen).", e);
}
}
public static KVRequest parseFrom(MessageFactory factory, byte[] data) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
return parseFrom(factory, data, cursor);
}
public static KVRequest parseFrom(MessageFactory factory, byte[] data, int offset, int length) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
cursor.addToPosition(offset);
cursor.setProcessUpToPosition(offset + length);
return parseFrom(factory, data, cursor);
}
public static KVRequest parseFrom(MessageFactory factory, byte[] data, CurrentCursor cursor) throws java.io.IOException {
KVRequest message = (KVRequest)factory.create("KVRequest");
if( message == null ) { 
throw new java.io.IOException("Factory create invalid message for type: KVRequest");
}
while(true) {
if (ProtobufInputStream.isAtEnd(data, cursor)) {
return message;
}
int varint = ProtobufInputStream.readRawVarint32(data, cursor);
int tag = ProtobufInputStream.getTagFieldNumber(varint);
switch(tag) {
case 0: 
return message;
 default: 
 ProtobufInputStream.skipUnknown(varint, data, cursor);
 break;
case 1: 
message.setCommand(ProtobufInputStream.readUint32(data,cursor));
break;
case 2: 
message.setKey(ProtobufInputStream.readBytes(data,cursor));
break;
case 3: 
message.setValue(ProtobufInputStream.readBytes(data,cursor));
break;
case 4: 
message.setVersion(ProtobufInputStream.readInt32(data,cursor));
break;
case 100: 
message.setServerAddress(ProtobufInputStream.readBytes(data,cursor));
break;
case 101: 
message.setServerPort(ProtobufInputStream.readUint32(data,cursor));
break;
case 102: 
message.setInformationTime(ProtobufInputStream.readUint64(data,cursor));
break;
}
}
}
/** Beware! All subsequent messages in stream will be consumed until end of stream (default protobuf behaivour).
  **/public static KVRequest parseFrom(MessageFactory factory, java.io.InputStream is) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
return parseFrom(factory, is, cursor);
}
public static KVRequest parseFrom(MessageFactory factory, java.io.InputStream is, int offset, int length) throws java.io.IOException {
CurrentCursor cursor = new CurrentCursor();
cursor.addToPosition(offset);
cursor.setProcessUpToPosition(offset + length);
return parseFrom(factory, is, cursor);
}
public static KVRequest parseFrom(MessageFactory factory, java.io.InputStream is, CurrentCursor cursor) throws java.io.IOException {
KVRequest message = (KVRequest)factory.create("KVRequest");
if( message == null ) { 
throw new java.io.IOException("Factory create invalid message for type: KVRequest");
}
while(true) {
if( cursor.getCurrentPosition() == cursor.getProcessUpToPosition() ) {
return message;
}
int varint = ProtobufInputStream.readRawVarint32(is, cursor);
int tag = ProtobufInputStream.getTagFieldNumber(varint);
if (ProtobufInputStream.isAtEnd(cursor)) {
return message;
}
switch(tag) {
case 0: 
return message;
 default: 
 ProtobufInputStream.skipUnknown(varint, is, cursor);
 break;case 1: 
message.setCommand(ProtobufInputStream.readUint32(is,cursor));
break;
case 2: 
message.setKey(ProtobufInputStream.readBytes(is,cursor));
break;
case 3: 
message.setValue(ProtobufInputStream.readBytes(is,cursor));
break;
case 4: 
message.setVersion(ProtobufInputStream.readInt32(is,cursor));
break;
case 100: 
message.setServerAddress(ProtobufInputStream.readBytes(is,cursor));
break;
case 101: 
message.setServerPort(ProtobufInputStream.readUint32(is,cursor));
break;
case 102: 
message.setInformationTime(ProtobufInputStream.readUint64(is,cursor));
break;
}
}
}
private static void assertInitialized(KVRequest message) {
if( !message.hasCommand()) {
throw new IllegalArgumentException("Required field not initialized: command");
}
}
}
