package com.chaconneai.spreader.discovery;

import com.chaconneai.spreader.util.NetworkUtils;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Parser for IPv4 address ranges: expands the various {@code ipAddressRange} forms
 * into a concrete list of addresses.
 *
 * <p>It works out <i>which addresses exist</i> and nothing more. What gets sent to them
 * and how is {@link StartEndIpRange}'s business.
 *
 * <p>Four forms are accepted:
 * <ul>
 *   <li>CIDR：{@code 192.168.1.0/24}</li>
 *   <li>Start-end pair, which may cross networks: {@code 192.168.0.1-192.168.5.254}</li>
 *   <li>Last-octet shorthand: {@code 192.168.1.10-200}</li>
 *   <li>A single address: {@code 192.168.1.5}</li>
 * </ul>
 *
 * <p>Expansion skips {@code x.x.x.0} (the network address) and {@code x.x.x.255}
 * (the broadcast address).
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class Ipv4Range {

    /**
     * Ceiling on how many addresses one expression may expand to, so that a mistyped
     * /8 cannot bring the scan to its knees.
     */
    private static final int MAX_ADDRESSES = 65_536;

    private final long startInclusive;
    private final long endInclusive;

    private Ipv4Range(long startInclusive, long endInclusive) {
        this.startInclusive = startInclusive;
        this.endInclusive = endInclusive;
    }

    /**
     * Parses a single range expression.
     */
    public static Ipv4Range parse(String expression) {
        String s = expression.trim();
        if (s.isEmpty()) {
            throw new IllegalArgumentException("range expression must not be empty");
        }

        int slash = s.indexOf('/');
        if (slash > 0) {
            return parseCidr(s, slash);
        }

        int dash = s.indexOf('-');
        if (dash > 0) {
            return parseDashRange(s, dash);
        }

        if (!NetworkUtils.isIpv4Literal(s)) {
            throw new IllegalArgumentException("malformed range expression: " + expression);
        }
        long v = NetworkUtils.ipToLong(s);
        return new Ipv4Range(v, v);
    }

    private static Ipv4Range parseCidr(String s, int slash) {
        String base = s.substring(0, slash).trim();
        int prefix;
        try {
            prefix = Integer.parseInt(s.substring(slash + 1).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed CIDR prefix: " + s, e);
        }
        if (prefix < 0 || prefix > 32) {
            throw new IllegalArgumentException("CIDR prefix out of range: " + s);
        }
        long ip = NetworkUtils.ipToLong(base);
        long mask = prefix == 0 ? 0L : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
        long network = ip & mask;
        long broadcast = network | (~mask & 0xFFFFFFFFL);
        return new Ipv4Range(network, broadcast);
    }

    private static Ipv4Range parseDashRange(String s, int dash) {
        String left = s.substring(0, dash).trim();
        String right = s.substring(dash + 1).trim();
        long start = NetworkUtils.ipToLong(left);
        long end;
        if (right.indexOf('.') < 0) {
            // Last-octet shorthand: 192.168.1.10-200
            int lastSeg;
            try {
                lastSeg = Integer.parseInt(right);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("malformed range expression: " + s, e);
            }
            if (lastSeg < 0 || lastSeg > 255) {
                throw new IllegalArgumentException("last octet out of range: " + s);
            }
            end = (start & 0xFFFFFF00L) | lastSeg;
        } else {
            end = NetworkUtils.ipToLong(right);
        }
        if (end < start) {
            throw new IllegalArgumentException("range ends before it starts: " + s);
        }
        return new Ipv4Range(start, end);
    }

    /**
     * Expands to the concrete address list, skipping network and broadcast addresses.
     */
    public List<String> expand() {
        long count = endInclusive - startInclusive + 1;
        if (count > MAX_ADDRESSES) {
            throw new IllegalArgumentException("range is too large (" + count + " addresses); "
                    + "split it up. The limit for one expression is " + MAX_ADDRESSES);
        }
        List<String> result = new ArrayList<>((int) count);
        for (long v = startInclusive; v <= endInclusive; v++) {
            long lastSeg = v & 0xFF;
            if (lastSeg == 0 || lastSeg == 255) {
                // A single-address expression (start == end) is taken at face value;
                // otherwise network and broadcast addresses are skipped
                if (startInclusive != endInclusive) {
                    continue;
                }
            }
            result.add(NetworkUtils.longToIp(v));
        }
        return result;
    }

    /**
     * Parses several expressions and expands them into one de-duplicated list.
     */
    public static List<String> expandAll(List<String> expressions) {
        Set<String> result = new LinkedHashSet<>();
        for (String expr : expressions) {
            result.addAll(parse(expr).expand());
        }
        return new ArrayList<>(result);
    }

    /**
     * Derives the /24 network a local address sits in.
     *
     * <p>Handy for building a scan range off whatever network this machine is on:
     * {@code .ipAddressRange(Ipv4Range.localSubnetOf(NetworkUtil.detectLocalAddress()).toString())}
     */
    public static Ipv4Range localSubnetOf(String localIp) {
        long v = NetworkUtils.ipToLong(localIp);
        long network = v & 0xFFFFFF00L;
        return new Ipv4Range(network, network | 0xFF);
    }

    @Override
    public String toString() {
        return NetworkUtils.longToIp(startInclusive) + "-" + NetworkUtils.longToIp(endInclusive);
    }
}
