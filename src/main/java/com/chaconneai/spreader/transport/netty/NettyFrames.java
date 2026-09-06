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
package com.chaconneai.spreader.transport.netty;

import com.chaconneai.spreader.protocol.Codec;
import com.chaconneai.spreader.protocol.Message;
import com.chaconneai.spreader.transport.Frames;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Frame coding for the Netty implementation.
 *
 * <h2>Netty splits the frames; their contents still go through {@link Codec}</h2>
 * Reassembling TCP's stream into frames is exactly what
 * {@link LengthFieldBasedFrameDecoder} exists for, and rewriting it would be pointless
 * -- one of the reasons for bringing Netty in at all. But <b>once a frame is split out,
 * it is parsed by the same {@link Codec} the built-in implementation uses</b>, so both
 * produce identical bytes on the wire and can share a cluster.
 *
 * <p>The header layout is in {@link Codec}: magic(4) + version(1) + type(1) +
 * payloadLen(4). The length field sits at offset 6 and excludes the header, hence a
 * {@code lengthAdjustment} of 0. {@code initialBytesToStrip} is 0 as well: the header is
 * kept so {@link Frames#decode} can check the magic and version itself, which saves
 * duplicating that validation here.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class NettyFrames {

    private NettyFrames() {
    }

    /** Splits frames by the length field. One instance per connection -- it carries
     *  accumulation state and must not be shared. */
    public static LengthFieldBasedFrameDecoder frameSplitter(int maxMessageBytes) {
        return new LengthFieldBasedFrameDecoder(
                maxMessageBytes + Codec.HEADER_LEN, 6, 4, 0, 0);
    }

    /** Turns a split frame into a {@link Message}. */
    public static ByteToMessageDecoder decoder(int maxMessageBytes) {
        return new ByteToMessageDecoder() {
            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                    throws Exception {
                if (!in.isReadable()) {
                    return;
                }
                // The previous handler has already guaranteed this is a complete frame
                ByteBuffer nio = in.nioBuffer();
                in.skipBytes(in.readableBytes());
                out.add(Frames.decode(nio, maxMessageBytes));
            }
        };
    }

    /** Stateless, so it may be shared across connections. */
    @ChannelHandler.Sharable
    public static final class Encoder extends MessageToByteEncoder<Message> {

        @Override
        protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out)
                throws Exception {
            out.writeBytes(Codec.encode(msg));
        }
    }

    /** Encodes into a standalone ByteBuf, which UDP drops straight into a datagram. */
    public static ByteBuf encode(Message msg) throws IOException {
        return Unpooled.wrappedBuffer(Codec.encode(msg));
    }
}
