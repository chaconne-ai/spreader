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
package com.chaconneai.spreader.protocol;

import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.NodeState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The binary codec: a hand-written compact format that depends on no serialisation
 * library and sidesteps the security problems of Java's native serialisation.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * +--------+---------+--------+------------+-----------------+
 * | magic  | version |  type  | payloadLen |     payload     |
 * | 4 byte | 1 byte  | 1 byte |   4 byte   |   payloadLen    |
 * +--------+---------+--------+------------+-----------------+
 * </pre>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class Codec {

    /** Magic number "GSSP", for cheaply recognising traffic that is not ours -- a port
     *  scanner's probe packets, for instance. */
    public static final int MAGIC = 0x47535350;

    /**
     * Protocol version.
     * <ul>
     *   <li>2 -- the body gained a business payload, and node information gained a
     *       "holds the cluster port" flag</li>
     *   <li>3 -- node information gained the application name, {@link Node#name()}</li>
     *   <li>4 -- added the one-way business message type
     *       {@link MessageType#PAYLOAD_ONEWAY}. The frame layout did not change, but older
     *       nodes do not recognise the type</li>
     *   <li>5 -- business messages gained a channel name, so a listener can subscribe to
     *       just the channels it cares about</li>
     * </ul>
     *
     * <p><b>There is no cross-version compatibility.</b> A message whose version differs
     * is dropped, so the whole cluster upgrades together. Running mixed versions shows up
     * as nodes not seeing one another -- not a fault, just versions that do not match.
     */
    public static final byte VERSION = 5;

    /** Length of the frame header. */
    public static final int HEADER_LEN = 10;

    private Codec() {
    }

    /** Encodes a message into a ByteBuffer ready to be written straight to a SocketChannel. */
    public static ByteBuffer encode(Message msg) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(msg.clusterName());
            out.writeLong(msg.seq());
            out.writeBoolean(msg.ok());
            out.writeUTF(msg.errorMessage());

            out.writeBoolean(msg.sender() != null);
            if (msg.sender() != null) {
                writeNode(out, msg.sender());
            }

            out.writeUTF(msg.targetId());
            out.writeUTF(msg.targetHost());
            out.writeInt(msg.targetPort());

            List<Node> members = msg.members();
            out.writeInt(members.size());
            for (Node n : members) {
                writeNode(out, n);
            }

            out.writeUTF(msg.channel());
            byte[] content = msg.content();
            out.writeInt(content.length);
            out.write(content);
        }

        byte[] payload = bos.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + payload.length);
        buf.putInt(MAGIC);
        buf.put(VERSION);
        buf.put(msg.type().code());
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();
        return buf;
    }

    /** Decodes a message body. The caller has already read and validated the header. */
    public static Message decode(byte typeCode, byte[] payload) throws IOException {
        MessageType type;
        try {
            type = MessageType.fromCode(typeCode);
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            String clusterName = in.readUTF();
            long seq = in.readLong();
            boolean ok = in.readBoolean();
            String errorMessage = in.readUTF();

            GossipMessage.Builder b = GossipMessage.builder(type, clusterName)
                    .seq(seq)
                    .ok(ok);
            if (!errorMessage.isEmpty()) {
                b.error(errorMessage);
                b.ok(ok);
            }

            if (in.readBoolean()) {
                b.sender(readNode(in));
            }

            String targetId = in.readUTF();
            String targetHost = in.readUTF();
            int targetPort = in.readInt();
            b.target(targetId, targetHost, targetPort);

            int count = in.readInt();
            if (count < 0 || count > 100_000) {
                throw new IOException("implausible member list length: " + count);
            }
            List<Node> members = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                members.add(readNode(in));
            }
            b.members(members);

            b.channel(in.readUTF());

            int contentLen = in.readInt();
            if (contentLen < 0 || contentLen > payload.length) {
                throw new IOException("implausible payload length: " + contentLen);
            }
            if (contentLen > 0) {
                byte[] content = new byte[contentLen];
                in.readFully(content);
                b.content(content);
            }

            return b.build();
        }
    }

    private static void writeNode(DataOutputStream out, Node n) throws IOException {
        out.writeUTF(n.id());
        out.writeUTF(n.name());
        out.writeUTF(n.host());
        out.writeInt(n.port());
        out.writeLong(n.startTime());
        out.writeLong(n.incarnation());
        out.writeByte(n.state().code());
        out.writeInt(n.priority());
        out.writeBoolean(n.clusterPortHolder());
        out.writeBoolean(n.onBreak());

        Map<String, String> meta = n.metadata();
        out.writeInt(meta.size());
        for (Map.Entry<String, String> e : meta.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue() == null ? "" : e.getValue());
        }
    }

    private static Node readNode(DataInputStream in) throws IOException {
        String id = in.readUTF();
        String name = in.readUTF();
        String host = in.readUTF();
        int port = in.readInt();
        long startTime = in.readLong();
        long incarnation = in.readLong();
        NodeState state;
        try {
            state = NodeState.fromCode(in.readByte());
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
        int priority = in.readInt();
        boolean clusterPortHolder = in.readBoolean();
        boolean onBreak = in.readBoolean();

        int metaSize = in.readInt();
        if (metaSize < 0 || metaSize > 1024) {
            throw new IOException("implausible metadata entry count: " + metaSize);
        }
        Map<String, String> meta = metaSize == 0 ? Map.of() : new LinkedHashMap<>();
        for (int i = 0; i < metaSize; i++) {
            meta.put(in.readUTF(), in.readUTF());
        }

        return new Node(id, name, host, port, startTime, incarnation, state, priority,
                clusterPortHolder, onBreak, meta);
    }
}
