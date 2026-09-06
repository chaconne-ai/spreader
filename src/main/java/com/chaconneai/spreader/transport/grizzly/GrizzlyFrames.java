/*
 * Copyright 2026 ChaconneAI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaconneai.spreader.transport.grizzly;

import com.chaconneai.spreader.protocol.Codec;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.transport.Frames;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.MemoryManager;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Frame coding for the Grizzly implementation.
 *
 * <h2>Stream framing follows Grizzly's filter idiom</h2>
 * Grizzly ships no ready-made length-field decoder, but its filter chain was designed
 * for exactly this: with an incomplete frame, {@code getStopAction(buffer)} hands the
 * bytes received back to the framework to hold, and they are fed in again together with
 * the next batch; once a frame is split out, {@code getInvokeAction(remainder)} leaves
 * the bytes behind it -- the next frame, arriving in the same read -- for the following
 * round. This is the standard Grizzly shape and requires no accumulation buffer of one's
 * own.
 *
 * <p>Once a frame is split out it is parsed by {@link Frames#decode}, shared by every
 * implementation, so the bytes on the wire are identical to the other three.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class GrizzlyFrames {

    private GrizzlyFrames() {
    }

    /** Encodes a message into a Grizzly buffer ready to be written out. */
    public static Buffer encode(Message msg) throws IOException {
        ByteBuffer nio = Codec.encode(msg);
        return Buffers.wrap(MemoryManager.DEFAULT_MEMORY_MANAGER, nio);
    }

    /**
     * The TCP framing filter.
     *
     * <p>Read direction only. Nothing is hung on the write direction: writing the encoded
     * buffer straight out is more direct, and saves a trip through the filter chain.
     */
    public static BaseFilter tcpDecoder(int maxMessageBytes) {
        return new BaseFilter() {
            @Override
            public NextAction handleRead(FilterChainContext ctx) throws IOException {
                Buffer buf = ctx.getMessage();
                if (buf == null || buf.remaining() < Codec.HEADER_LEN) {
                    // Not even a full header yet; hand it back for the framework to accumulate
                    return ctx.getStopAction(buf);
                }
                // The length field sits at offset 6 in the header and excludes the header
                int payloadLen = buf.getInt(buf.position() + 6);
                if (payloadLen < 0 || payloadLen > maxMessageBytes) {
                    throw new IOException("bad message length: " + payloadLen);
                }
                int frameLen = Codec.HEADER_LEN + payloadLen;
                if (buf.remaining() < frameLen) {
                    return ctx.getStopAction(buf);
                }

                // One read may carry several frames back to back; slice off the surplus
                // and leave it for the next round
                Buffer remainder = buf.remaining() > frameLen
                        ? buf.split(buf.position() + frameLen)
                        : null;

                byte[] frame = new byte[frameLen];
                buf.get(frame);
                ctx.setMessage(Frames.decode(ByteBuffer.wrap(frame), maxMessageBytes));
                return ctx.getInvokeAction(remainder);
            }
        };
    }
}
