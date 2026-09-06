package com.chaconneai.spreader.transport.mina;

import com.chaconneai.spreader.protocol.Codec;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.transport.Frames;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Frame coding for the MINA implementation.
 *
 * <h2>MINA's cumulative decoder handles the stream framing</h2>
 * The protocol has the standard <b>fixed header plus length field</b> layout
 * ({@link Codec}: magic, version, type, payloadLen), the same approach Dubbo and
 * RocketMQ take. The length lives at a fixed offset, so the decoder reads it and then
 * checks whether a whole frame has arrived -- no delimiter scanning, and therefore no
 * pathological case.
 *
 * <p>{@link org.apache.mina.filter.codec.CumulativeProtocolDecoder} holds on to bytes
 * that do not yet form a frame and brings them back next time, which is precisely what
 * TCP's partial reads call for, so no buffer management has to be written here. Once a
 * frame is split out it is parsed by {@link Frames#decode}, shared by every
 * implementation -- which is why the bytes on the wire are identical to the other
 * three.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class MinaFrames {

    private MinaFrames() {
    }

    /** The codec factory used for TCP. */
    public static ProtocolCodecFactory codecFactory(int maxMessageBytes) {
        ProtocolEncoder encoder = new ProtocolEncoderAdapter() {
            @Override
            public void encode(IoSession session, Object message, ProtocolEncoderOutput out)
                    throws Exception {
                out.write(IoBuffer.wrap(Codec.encode((Message) message)));
            }
        };
        ProtocolDecoder decoder = new FrameDecoder(maxMessageBytes);
        return new ProtocolCodecFactory() {
            @Override
            public ProtocolEncoder getEncoder(IoSession session) {
                return encoder;
            }

            @Override
            public ProtocolDecoder getDecoder(IoSession session) {
                return decoder;
            }
        };
    }

    /**
     * Splits frames by the length field.
     *
     * <p>{@code CumulativeProtocolDecoder} guarantees the argument holds every byte
     * received so far; returning false means there is not yet enough and it should keep
     * accumulating.
     */
    private static final class FrameDecoder extends CumulativeProtocolDecoder {

        private final int maxMessageBytes;

        FrameDecoder(int maxMessageBytes) {
            this.maxMessageBytes = maxMessageBytes;
        }

        @Override
        protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
                throws Exception {
            if (in.remaining() < Codec.HEADER_LEN) {
                return false;
            }
            // The length field sits at offset 6 in the header and excludes the header
            int payloadLen = in.getInt(in.position() + 6);
            if (payloadLen < 0 || payloadLen > maxMessageBytes) {
                throw new IOException("bad message length: " + payloadLen);
            }
            int frameLen = Codec.HEADER_LEN + payloadLen;
            if (in.remaining() < frameLen) {
                return false;
            }
            byte[] frame = new byte[frameLen];
            in.get(frame);
            out.write(Frames.decode(ByteBuffer.wrap(frame), maxMessageBytes));
            return true;
        }
    }
}
