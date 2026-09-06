package com.chaconneai.spreader.transport;

/**
 * Which framework implements the transport.
 *
 * <p>Orthogonal to {@link TransportType} (TCP or UDP): that decides <b>which
 * protocol</b>, this decides <b>who moves the bytes</b>. Protocol semantics, frame
 * format and port-claiming rules are identical across all four, so <b>nodes running
 * different implementations can share one cluster</b> -- which is also what makes
 * comparative benchmarking possible.
 *
 * <h2>Choosing one</h2>
 * <b>{@link #NIO} by default</b>: the built-in implementation, no third-party
 * dependency, runs with nothing added. It is also the reference implementation
 * behaviourally -- the other three are held to what it does.
 *
 * <p>To use another, add the corresponding jar yourself; inside spreader they are all
 * {@code optional} and are not inherited. Configuring one whose library is absent falls
 * back to {@link #NIO} with a warn line, rather than failing startup.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum TransportProvider {

    /**
     * The hand-written plain NIO implementation, with no third-party dependency.
     *
     * <p>TCP uses a Selector event loop plus a connection pool; UDP fragmentation and
     * reassembly are {@link DatagramFragmenter}'s job.
     */
    NIO("", "com.chaconneai.spreader.transport.nio.Nio"),

    /**
     * Netty. Requires adding {@code io.netty:netty-all} yourself, or netty-handler plus
     * netty-transport.
     */
    NETTY("io.netty.channel.Channel",
            "com.chaconneai.spreader.transport.netty.Netty"),

    /**
     * Apache MINA。
     *
     * <p>Requires {@code org.apache.mina:mina-core}.
     */
    MINA("org.apache.mina.core.service.IoService",
            "com.chaconneai.spreader.transport.mina.Mina"),

    /**
     * Grizzly。
     *
     * <p>Requires {@code org.glassfish.grizzly:grizzly-framework}.
     */
    GRIZZLY("org.glassfish.grizzly.Connection",
            "com.chaconneai.spreader.transport.grizzly.Grizzly");

    private final String probeClass;
    private final String implPrefix;

    TransportProvider(String probeClass, String implPrefix) {
        this.probeClass = probeClass;
        this.implPrefix = implPrefix;
    }

    /** The probe class used to tell whether this framework is on the classpath.
     *  {@link #NIO} returns the empty string. */
    public String probeClass() {
        return probeClass;
    }

    /** Prefix of the implementation class name, followed by {@code TcpTransport} or
     *  {@code UdpTransport}. */
    public String implPrefix() {
        return implPrefix;
    }

    /** Whether the classes this implementation needs are on the classpath. */
    public boolean available() {
        if (this == NIO) {
            return true;
        }
        try {
            Class.forName(probeClass, false, TransportProvider.class.getClassLoader());
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
