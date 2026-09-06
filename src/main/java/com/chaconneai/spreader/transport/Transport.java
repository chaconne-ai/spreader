package com.chaconneai.spreader.transport;

import com.chaconneai.spreader.protocol.Message;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The transport abstraction.
 *
 * <p>Everything above depends only on this interface, so changing protocol (TCP or UDP)
 * or framework (built-in NIO, Netty, MINA, Grizzly) is entirely transparent to the
 * cluster logic. Which one gets built is {@link TransportFactory}'s decision; see
 * {@link TransportType} and {@link TransportProvider}.
 *
 * <p><b>Every implementation must behave identically on the wire</b>: same frame format,
 * same port-claiming rules, same request/response semantics. That is what lets nodes
 * running different implementations share one cluster -- which is how the comparative
 * benchmarks are run.
 *
 * <h2>Two kinds of port</h2>
 * <ul>
 *   <li><b>The work port</b>, bound during {@link #start()}. Every node has its own, and
 *       it is the address the node publishes in the member list. Several instances on one
 *       machine step past each other automatically, so they never collide.</li>
 *   <li><b>The cluster port</b>, the fixed port claimed by {@link #open(int)} (22000 by
 *       default). Exactly one node cluster-wide can hold it; the holder is the leader,
 *       and it is also the door every new node knocks on.</li>
 * </ul>
 *
 * <p>{@link #open(int)} <i>claims</i> rather than <i>binds</i> because its return value
 * is itself the election result. On one machine the operating system guarantees
 * exclusion; across machines the guarantee comes from the order of operations -- look
 * first, claim second, and only claim after a {@code lookup} has found no holder.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface Transport {

    /** Sets the inbound message handler. Must be called before {@link #start()}. */
    void setHandler(MessageHandler handler);

    /**
     * Binds the work port and starts sending and receiving.
     *
     * @throws IOException when every candidate port is taken
     */
    void start() throws IOException;

    /** Stops sending and receiving and releases every port, the claimed cluster port included. */
    void stop();

    /**
     * Claims a port.
     *
     * <p>Once claimed, messages arriving there go to the same handler as those arriving
     * at the work port, so the caller need not distinguish them. Claiming the same port
     * again is idempotent.
     *
     * @return the address actually bound on success, or null when another process already
     *         holds the port. That is not an error -- it is the normal outcome of the
     *         competition
     */
    InetSocketAddress open(int port);

    /** Releases a previously claimed port. Does nothing when it is not held. */
    void close(int port);

    /** Whether this node currently holds the port. */
    boolean holds(int port);

    /** The work port -- the port this node publishes to everyone else. */
    int boundPort();

    /**
     * Checks the work port and rebinds it in place if it has failed.
     *
     * <h2>Why this is needed</h2>
     * Once the work port's listening socket fails -- the interface went down, the kernel
     * closed it, some code path closed it by mistake -- the node <b>receives nothing ever
     * again</b>, and has no idea:
     *
     * <ul>
     *   <li>The member list is updated by receiving messages. Receiving nothing, it stays
     *       frozen on an old snapshot, so {@code memberList.size() > 1} holds forever and
     *       the "rediscover only when alone" path <b>never fires</b></li>
     *   <li>The node still sends probes -- sending works fine -- and others still reply,
     *       but the replies never arrive. So it proceeds to declare <b>the entire
     *       cluster</b> failed, one node at a time</li>
     * </ul>
     *
     * <p>The symptom is a node that is alive but deaf, with a log full of normality.
     * A pure silent failure: without this check, nothing at all would find it.
     *
     * <h2>Why rebind the <i>same</i> port</h2>
     * The work port has already propagated cluster-wide with the member list. Moving to a
     * different port would invalidate the address everyone knows and require rejoining
     * and rebroadcasting -- a different mechanism entirely. Rebinding the original is
     * <b>completely invisible from outside</b>, and very likely to succeed: a dead socket
     * does not mean someone else took the port.
     *
     * <p>If it cannot be rebound -- the port really is taken -- this returns
     * {@code false} so the layer above learns this node is finished, rather than carrying
     * on pretending to be healthy.
     *
     * @return {@code true} when the work port is usable, whether it was fine all along or
     *         has just been repaired
     */
    default boolean checkAndRepairWorkPort() {
        return true;
    }

    boolean isRunning();

    /** A monotonically increasing message sequence number, used to match responses to requests. */
    long nextSeq();

    /**
     * Issues a request and waits for the response.
     *
     * <p>Over TCP an existing connection is reused where possible. Reuse is safe: should
     * a borrowed connection turn out to have been closed by the peer, the call is retried
     * once on a fresh one.
     *
     * @param timeoutMs total read/write timeout
     * @throws IOException on connection failure, timeout, or an error at the peer
     */
    Message request(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException;

    /**
     * Issues a request <b>without using the connection pool</b>.
     *
     * <p>For one-shot addresses such as those in a range scan: a scan hits a great many
     * hosts that do not exist, and pooling those would only poison the pool.
     */
    Message requestDirect(InetSocketAddress addr, Message request, int timeoutMs)
            throws IOException;

    /**
     * Sends one way, without waiting for a response. Used for departure notices and for
     * business messages that want no reply.
     *
     * <p><b>Note</b>: the peer must genuinely send nothing back. Otherwise the response
     * sits on the connection and the next request reads it as its own, putting every
     * response out of step. The protocol expresses "do not answer me" with dedicated
     * message types such as
     * {@link com.chaconneai.spreader.protocol.MessageType#PAYLOAD_ONEWAY}.
     */
    void send(InetSocketAddress addr, Message message, int timeoutMs) throws IOException;
}
