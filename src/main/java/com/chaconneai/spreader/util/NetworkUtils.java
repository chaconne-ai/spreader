package com.chaconneai.spreader.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Network helpers: local address detection, address parsing, and conversion between
 * dotted-quad IPs and 32-bit integers.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class NetworkUtils {

    private NetworkUtils() {
    }

    /**
     * Detects the IPv4 address this machine presents to the outside world.
     *
     * <p>It prefers a private address on an interface that is UP, not loopback and not
     * virtual, in the order 192.168 &gt; 10. &gt; 172.16-31 &gt; anything else.
     *
     * @return an address such as {@code 192.168.1.20}, falling back to {@code 127.0.0.1}
     *         when nothing suitable is found
     */
    public static String detectLocalAddress() {
        List<Inet4Address> candidates = new ArrayList<>();
        try {
            for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                try {
                    if (!nif.isUp() || nif.isLoopback() || nif.isVirtual() || nif.isPointToPoint()) {
                        continue;
                    }
                } catch (SocketException ignore) {
                    continue;
                }
                for (InetAddress addr : Collections.list(nif.getInetAddresses())) {
                    if (addr instanceof Inet4Address v4 && !v4.isLoopbackAddress() && !v4.isLinkLocalAddress()) {
                        candidates.add(v4);
                    }
                }
            }
        } catch (SocketException e) {
            // Ignored; fall through to the fallback below
        }

        if (candidates.isEmpty()) {
            return "127.0.0.1";
        }
        candidates.sort(Comparator.comparingInt(NetworkUtils::addressRank));
        return candidates.get(0).getHostAddress();
    }

    /**
     * Address ranking: the lower the number, the more preferred.
     */
    private static int addressRank(Inet4Address addr) {
        String ip = addr.getHostAddress();
        if (ip.startsWith("192.168.")) {
            return 0;
        }
        if (ip.startsWith("10.")) {
            return 1;
        }
        if (addr.isSiteLocalAddress()) {
            return 2;
        }
        return 3;
    }

    /**
     * Parses an address written as {@code host} or {@code host:port}.
     *
     * @param text        the text to parse
     * @param defaultPort the port to use when the text does not specify one
     */
    public static InetSocketAddress parseAddress(String text, int defaultPort) {
        String s = text.trim();
        if (s.isEmpty()) {
            throw new IllegalArgumentException("address must not be empty");
        }
        int idx = s.lastIndexOf(':');
        if (idx < 0) {
            return new InetSocketAddress(s, defaultPort);
        }
        String host = s.substring(0, idx).trim();
        String portText = s.substring(idx + 1).trim();
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed port: " + text, e);
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + text);
        }
        return new InetSocketAddress(host, port);
    }

    /**
     * Dotted quad to 32-bit integer.
     */
    public static long ipToLong(String ip) {
        String[] parts = ip.trim().split("\\.");
        if (parts.length != 4) {
            throw new IllegalArgumentException("malformed IPv4 address: " + ip);
        }
        long v = 0;
        for (String part : parts) {
            int seg = Integer.parseInt(part.trim());
            if (seg < 0 || seg > 255) {
                throw new IllegalArgumentException("malformed IPv4 address: " + ip);
            }
            v = (v << 8) | seg;
        }
        return v;
    }

    /**
     * 32-bit integer to dotted quad.
     */
    public static String longToIp(long v) {
        return ((v >> 24) & 0xFF) + "." + ((v >> 16) & 0xFF) + "." + ((v >> 8) & 0xFF) + "." + (v & 0xFF);
    }

    /**
     * Whether the text is an IPv4 literal such as {@code 1.2.3.4}.
     */
    public static boolean isIpv4Literal(String s) {
        String[] parts = s.trim().split("\\.");
        if (parts.length != 4) {
            return false;
        }
        for (String p : parts) {
            try {
                int seg = Integer.parseInt(p.trim());
                if (seg < 0 || seg > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }
}
