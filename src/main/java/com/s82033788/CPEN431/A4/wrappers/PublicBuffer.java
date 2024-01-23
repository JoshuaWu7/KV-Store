package com.s82033788.CPEN431.A4.wrappers;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PublicBuffer {
    private byte[] buf;

    private int len;
    private PB_ContentType contentType;
    private int idOffset;
    Closeable currentStream;


    public PublicBuffer(byte[] backingArray, PB_ContentType type, int length) {
        buf = backingArray;
        len = length;
        contentType = type;
    }

    public int getLen() {
        checkStreamIsClosed();
        return len;
    }

    public int getPayloadLength() {
        checkStreamIsClosed();
        if (contentType != PB_ContentType.PAYLOADNID)
            throw new ContentMismatchException("The public buffer does not have what you are expecting");

        return len - idOffset;

    }

    public PB_InputStream readPacketFromPB() {
        checkStreamIsClosed();
        if (contentType != PB_ContentType.PACKET)
            throw new ContentMismatchException("The public buffer does not have what you are expecting");

        currentStream = new PB_InputStream(buf, 0, len);
        return (PB_InputStream) currentStream;
    }

    public PB_OutputStream writeIDToPB() {
        checkStreamIsClosed();
        contentType = PB_ContentType.ID;
        return (PB_OutputStream) (currentStream = new PB_OutputStream(buf));
    }

    public PB_OutputStream writePayloadToPB() {
        checkStreamIsClosed();
        if(contentType != PB_ContentType.ID)
            throw new ContentMismatchException("The public buffer does not have what you are expecting");

        contentType = PB_ContentType.PAYLOADNID;
        return (PB_OutputStream) (currentStream = new PB_OutputStream(buf, idOffset));//return output stream offset by current length set by writeIDToPB;
    }

    public ByteBuffer borrowBodyForCRCFromPB() {
        checkStreamIsClosed();
        if (contentType != PB_ContentType.PAYLOADNID)
            throw new ContentMismatchException("The public buffer does not have what you are expecting");

        return ByteBuffer.wrap(buf, 0, len);
    }


    /**
     * By running this function, the user promises not to keep any more copies of the
     * ByteBuffer / underlying byte array borrowed previously, nor to the underlying byte array.
     * If they do, they will be sent to hell.
     */
    public void returnBodyToPB() {
    }

    public PB_InputStream readPayloadFromPB() {
        checkStreamIsClosed();
        if (contentType != PB_ContentType.PAYLOADNID)
            throw new ContentMismatchException("The public buffer does not have what you are expecting");

        return (PB_InputStream) (currentStream = new PB_InputStream(buf, idOffset, len - idOffset));
    }

    public PB_OutputStream writeValueToPB() {
        checkStreamIsClosed();

        contentType = PB_ContentType.VALUE;
        return (PB_OutputStream) (currentStream = new PB_OutputStream(buf));
    }

    public byte[] getValueCopy() {
        checkStreamIsClosed();
        if (contentType != PB_ContentType.VALUE)
            throw new ContentMismatchException("The public buffer does not have what you are expecting");

        return Arrays.copyOf(buf, len);
    }

    private void checkStreamIsClosed()
    {
        if (currentStream != null) {
            try {
                currentStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public class PB_OutputStream extends OutputStream {
       private ByteBuffer buffer;
        private PB_OutputStream(byte[] buf) {
            buffer = ByteBuffer.wrap(buf);
        }

        private PB_OutputStream(byte[] buf, int off) {
            buffer = ByteBuffer.wrap(buf, off, buf.length - off);
        }


        @Override
        public void close() throws IOException {
            super.close();

            if(buffer != null)
            {
                buffer.flip();
                PublicBuffer.this.len = buffer.limit();

                if(PublicBuffer.this.contentType == PB_ContentType.ID) PublicBuffer.this.idOffset = buffer.limit();

                buffer = null;
            }
        }

        @Override
        public void write(int b) throws IOException {

            if(buffer != null)
            {
                buffer.put((byte) b);
            }
        }
    }

    public class PB_InputStream extends InputStream
    {
        ByteBuffer buf;
        public PB_InputStream(byte[] buffer, int off, int len) {
            super();
            buf = ByteBuffer.wrap(buffer, off, len);

        }


        @Override
        public int available() throws IOException {
            return buf.limit() - buf.position();
        }

        @Override
        public void close() throws IOException {
            super.close();
            buf = null;
        }

        @Override
        public void mark(int readlimit) {
            buf.mark();
        }

        @Override
        public void reset() throws IOException {
            buf.reset();
        }

        @Override
        public boolean markSupported() {
            super.markSupported();
            return true;
        }

        @Override
        public long transferTo(OutputStream out) throws IOException {
            return super.transferTo(out);
        }

        @Override
        public int read() throws IOException {
            return buf.hasRemaining() ? (buf.get() & 0xff) : -1;
        }
    }

    public static final class ContentMismatchException extends IllegalStateException {
        public ContentMismatchException(String s) {
            super(s);
        }
    }
    public static final class StreamOpenException extends IllegalStateException {
        public StreamOpenException(String s) {
            super(s);
        }
    }


}

