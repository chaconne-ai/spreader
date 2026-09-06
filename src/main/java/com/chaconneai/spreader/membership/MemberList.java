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
package com.chaconneai.spreader.membership;

import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.NodeState;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The membership view: the single authoritative source of cluster state.
 *
 * <p>Every change happens under one mutex, which makes "merge, compute the changes,
 * recompute the leader" a single atomic operation. Reads go through a lock-free immutable
 * snapshot, so in this read-mostly workload they cost very little.
 *
 * <p>State merging follows SWIM's standard semantics:
 * <ul>
 *   <li>information with a higher incarnation is always newer and overrides outright;</li>
 *   <li>at equal incarnation, the more certain state wins, in the order
 *       ALIVE &lt; SUSPECT &lt; DEAD &lt; LEFT;</li>
 *   <li>on receiving a non-ALIVE judgement about itself, a node increments its own
 *       incarnation and broadcasts ALIVE to refute it -- which repairs misjudgements
 *       caused by network jitter automatically.</li>
 * </ul>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MemberList {

    private final ReentrantLock lock = new ReentrantLock();
    private final Logger log;
    private final long tombstoneTtlMs;

    /** This node's own information; the incarnation increments with each refutation. */
    private final AtomicReference<Node> self;

    /** nodeId -> entry, including departed nodes still within their tombstone period. */
    private final Map<String, Entry> entries = new HashMap<>();

    /** The immutable snapshot exposed outward, so reads need no lock. */
    private volatile List<Node> membersSnapshot = List.of();
    private volatile List<Node> allSnapshot = List.of();
    private volatile Node leader;

    public MemberList(Node self, long tombstoneTtlMs, Logger log) {
        this.self = new AtomicReference<>(Objects.requireNonNull(self, "self"));
        this.tombstoneTtlMs = tombstoneTtlMs;
        this.log = log;
        refreshSnapshots();
    }

    /** This node's current information. */
    public Node self() {
        return self.get();
    }

    /** Every effective member (ALIVE and SUSPECT), this node included, in takeover order. */
    public List<Node> members() {
        return membersSnapshot;
    }

    /** Every entry, including departed nodes in their tombstone period. Used to gossip
     *  departures onward. */
    public List<Node> allEntries() {
        return allSnapshot;
    }

    /**
     * The current leader -- that is, the node holding the cluster port.
     *
     * <p>Note this is <i>observed</i> rather than <i>computed</i>: a node claims the
     * cluster port and gossips that fact onward, and every other node takes it as given.
     * It returns null during the gap after a leader departs and before a new one has
     * claimed the port.
     *
     * <p>Normally exactly one member qualifies. More than one means the view has split --
     * several machines that could not see one another, each holding the cluster port on
     * its own host. The one earliest in the takeover order is chosen, which makes every
     * node agree; the one behind it releases its port and yields on seeing this view.
     */
    public Node leader() {
        return leader;
    }

    public boolean isLeader() {
        Node l = leader;
        return l != null && l.id().equals(self.get().id());
    }

    /**
     * The member first in the takeover order.
     *
     * <p>It is the one that goes for the cluster port after a leader departs. Every node
     * computes the same answer (see {@link Node#compareTo}), so nothing is negotiated --
     * the first steps forward and the rest simply wait.
     */
    public Node first() {
        List<Node> members = membersSnapshot;
        return members.isEmpty() ? null : members.get(0);
    }

    /** Whether this node is first in the takeover order. */
    public boolean isFirst() {
        Node f = first();
        return f != null && f.id().equals(self.get().id());
    }

    /**
     * This node's rank in the takeover order, 0 being first.
     * Used to stagger when each node attempts a takeover, so they do not all go for the
     * port at once.
     */
    public int selfRank() {
        String selfId = self.get().id();
        List<Node> members = membersSnapshot;
        for (int i = 0; i < members.size(); i++) {
            if (members.get(i).id().equals(selfId)) {
                return i;
            }
        }
        return members.size();
    }

    /** How many effective members there are, this node included. */
    public int size() {
        return membersSnapshot.size();
    }

    public Node get(String nodeId) {
        lock.lock();
        try {
            Entry e = entries.get(nodeId);
            return e == null ? null : e.node;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Picks some probeable nodes at random, excluding this node and any tombstones.
     *
     * @param count   how many are wanted
     * @param exclude a node ID to leave out; may be null
     */
    public List<Node> randomMembers(int count, String exclude) {
        List<Node> candidates = new ArrayList<>(membersSnapshot.size());
        String selfId = self.get().id();
        for (Node n : membersSnapshot) {
            if (n.id().equals(selfId) || n.id().equals(exclude)) {
                continue;
            }
            candidates.add(n);
        }
        if (candidates.size() <= count) {
            return candidates;
        }
        // A partial Fisher-Yates: only the first `count` are shuffled
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++) {
            int j = rnd.nextInt(i, candidates.size());
            Node tmp = candidates.get(i);
            candidates.set(i, candidates.get(j));
            candidates.set(j, tmp);
        }
        return new ArrayList<>(candidates.subList(0, count));
    }

    /**
     * Merges a membership view received from a peer.
     *
     * @param incoming the nodes in the peer's view
     * @return the changes this merge produced; the caller publishes events from them
     */
    public MergeResult merge(Collection<Node> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return MergeResult.EMPTY;
        }
        long now = System.currentTimeMillis();
        List<MembershipChange> changes = new ArrayList<>(2);
        boolean refuted = false;

        lock.lock();
        try {
            for (Node in : incoming) {
                if (in.id().equals(self.get().id())) {
                    refuted |= refuteIfNeeded(in);
                    continue;
                }
                applyOne(in, now, changes);
            }
            if (!changes.isEmpty() || refuted) {
                refreshSnapshots();
            }
        } finally {
            lock.unlock();
        }
        return new MergeResult(changes, refuted);
    }

    /** Merges one node. */
    private void applyOne(Node in, long now, List<MembershipChange> changes) {
        Entry existing = entries.get(in.id());

        if (existing == null) {
            // An older instance may exist at the same address: a peer process that restarted
            // takes on a new ID
            Entry sameEndpoint = findByEndpoint(in);
            if (sameEndpoint != null) {
                if (sameEndpoint.node.startTime() > in.startTime()) {
                    // This is information about an older instance; discard it
                    return;
                }
                // The old instance has been superseded by the new one; treat it as departed
                Entry removed = entries.remove(sameEndpoint.node.id());
                if (removed != null && removed.node.state().isMember()) {
                    Node dead = removed.node.withState(NodeState.DEAD);
                    changes.add(MembershipChange.of(MembershipChange.Type.DEAD, dead, removed.node));
                    log.info("Node {} has been superseded by a new instance at the same address "
                            + "and is declared departed", removed.node.address());
                }
            }

            if (in.state().isTerminal()) {
                // A departed node never seen before: keep a tombstone only, and produce no
                // join or departure event
                entries.put(in.id(), new Entry(in, now));
                return;
            }
            entries.put(in.id(), new Entry(in, now));
            changes.add(MembershipChange.joined(in));
            log.info("Node joined: {}", in);
            return;
        }

        Node current = existing.node;
        if (!shouldOverride(current, in)) {
            return;
        }

        existing.node = in;
        existing.stateChangeMs = now;
        MembershipChange.Type type = classify(current, in);
        if (type != null) {
            changes.add(MembershipChange.of(type, in, current));
            if (type != MembershipChange.Type.UPDATED) {
                log.info("Node state change [{}]: {} -> {}", in.address(), current.state(), in.state());
            }
        }
    }

    /** Whether the incoming information may override what is held now. */
    private static boolean shouldOverride(Node current, Node in) {
        if (in.incarnation() > current.incarnation()) {
            return true;
        }
        if (in.incarnation() < current.incarnation()) {
            return false;
        }
        return in.state().priority() > current.state().priority();
    }

    /** Classifies the change from the before and after states. */
    private static MembershipChange.Type classify(Node before, Node after) {
        if (before.state() == after.state()) {
            // A change of cluster-port ownership has to count as a change too, or a
            // handover of leadership would be missed by the "no change, no snapshot
            // refresh" rule
            boolean changed = !before.metadata().equals(after.metadata())
                    || before.clusterPortHolder() != after.clusterPortHolder()
                    || before.onBreak() != after.onBreak();
            return changed ? MembershipChange.Type.UPDATED : null;
        }
        return switch (after.state()) {
            case ALIVE -> before.state() == NodeState.SUSPECT
                    ? MembershipChange.Type.RECOVERED
                    : MembershipChange.Type.JOINED;
            case SUSPECT -> MembershipChange.Type.SUSPECT;
            case DEAD -> MembershipChange.Type.DEAD;
            case LEFT -> MembershipChange.Type.LEFT;
        };
    }

    private Entry findByEndpoint(Node in) {
        for (Entry e : entries.values()) {
            if (e.node.sameEndpoint(in)) {
                return e;
            }
        }
        return null;
    }

    /**
     * Refutes a negative judgement about this node: increments the incarnation past the
     * peer's and declares ALIVE again.
     *
     * @return whether a refutation was produced, which must be broadcast at once
     */
    private boolean refuteIfNeeded(Node claim) {
        Node me = self.get();
        if (claim.state() == NodeState.ALIVE || claim.incarnation() < me.incarnation()) {
            return false;
        }
        Node updated = me.withState(NodeState.ALIVE, claim.incarnation() + 1);
        self.set(updated);
        log.warn("Received a {} judgement about this node; incarnation raised to {} and a "
                + "refutation broadcast",
                claim.state(), updated.incarnation());
        return true;
    }

    /**
     * Marks a node SUSPECT locally.
     *
     * @return the change produced, or null when nothing changed
     */
    public MembershipChange markSuspect(String nodeId) {
        lock.lock();
        try {
            Entry e = entries.get(nodeId);
            if (e == null || e.node.state() != NodeState.ALIVE) {
                return null;
            }
            Node before = e.node;
            e.node = before.withState(NodeState.SUSPECT);
            e.stateChangeMs = System.currentTimeMillis();
            refreshSnapshots();
            log.warn("Node is suspect: {}", before.address());
            return MembershipChange.of(MembershipChange.Type.SUSPECT, e.node, before);
        } finally {
            lock.unlock();
        }
    }

    /** Confirms a node is alive after a successful probe. */
    public MembershipChange markAlive(String nodeId) {
        lock.lock();
        try {
            Entry e = entries.get(nodeId);
            if (e == null || e.node.state() != NodeState.SUSPECT) {
                return null;
            }
            Node before = e.node;
            // Raise the incarnation so this recovery overrides any SUSPECT still
            // propagating through the network
            e.node = before.withState(NodeState.ALIVE, before.incarnation() + 1);
            e.stateChangeMs = System.currentTimeMillis();
            refreshSnapshots();
            log.info("Node recovered: {}", before.address());
            return MembershipChange.of(MembershipChange.Type.RECOVERED, e.node, before);
        } finally {
            lock.unlock();
        }
    }

    /** Declares a node dead locally. */
    public MembershipChange markDead(String nodeId) {
        lock.lock();
        try {
            Entry e = entries.get(nodeId);
            if (e == null || !e.node.state().isMember()) {
                return null;
            }
            Node before = e.node;
            e.node = before.withState(NodeState.DEAD, before.incarnation() + 1);
            e.stateChangeMs = System.currentTimeMillis();
            refreshSnapshots();
            log.warn("Node departed after failure: {}", before.address());
            return MembershipChange.of(MembershipChange.Type.DEAD, e.node, before);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Scans for nodes whose SUSPECT state has timed out, moves them to DEAD, and sweeps
     * expired tombstones.
     *
     * @param suspectTimeoutMs the longest a node may stay SUSPECT
     */
    public List<MembershipChange> reapExpired(long suspectTimeoutMs) {
        long now = System.currentTimeMillis();
        List<MembershipChange> changes = new ArrayList<>(1);
        boolean dirty = false;

        lock.lock();
        try {
            var it = entries.entrySet().iterator();
            while (it.hasNext()) {
                Entry e = it.next().getValue();
                if (e.node.state() == NodeState.SUSPECT
                        && now - e.stateChangeMs >= suspectTimeoutMs) {
                    Node before = e.node;
                    e.node = before.withState(NodeState.DEAD, before.incarnation() + 1);
                    e.stateChangeMs = now;
                    changes.add(MembershipChange.of(MembershipChange.Type.DEAD, e.node, before));
                    log.warn("A suspect node timed out and is declared departed: {}", before.address());
                    dirty = true;
                } else if (e.node.state().isTerminal() && now - e.stateChangeMs >= tombstoneTtlMs) {
                    // The tombstone has expired; forget it entirely. Should that node
                    // reappear later it will be treated as wholly new
                    it.remove();
                    dirty = true;
                }
            }
            if (dirty) {
                refreshSnapshots();
            }
        } finally {
            lock.unlock();
        }
        return changes;
    }

    /** Marks this node as having left gracefully, for the shutdown broadcast. */
    public Node markSelfLeft() {
        lock.lock();
        try {
            Node updated = self.get().withState(NodeState.LEFT, self.get().incarnation() + 1);
            self.set(updated);
            refreshSnapshots();
            return updated;
        } finally {
            lock.unlock();
        }
    }

    /** Updates this node's business metadata; the change gossips out to the whole cluster. */
    public Node updateSelfMetadata(Map<String, String> metadata) {
        lock.lock();
        try {
            Node me = self.get();
            Node updated = new Node(me.id(), me.name(), me.host(), me.port(), me.startTime(),
                    me.incarnation() + 1, me.state(), me.priority(),
                    me.clusterPortHolder(), me.onBreak(), metadata);
            self.set(updated);
            refreshSnapshots();
            return updated;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Records that this node has claimed or released the cluster port.
     *
     * <p>This is the sole source of leader identity: the node updates itself the moment it
     * claims the port, and gossip tells everyone else. The incarnation must be raised, or
     * this new information would be overwritten by older information still propagating.
     *
     * @return the updated node information, or null when nothing changed
     */
    public Node updateSelfClusterPort(boolean holding) {
        lock.lock();
        try {
            Node me = self.get();
            if (me.clusterPortHolder() == holding) {
                return null;
            }
            Node updated = me.withClusterPort(holding, me.incarnation() + 1);
            self.set(updated);
            refreshSnapshots();
            log.info(holding ? "This node now holds the cluster port and is the leader"
                    : "This node has released the cluster port");
            return updated;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Records this node entering or leaving rest.
     *
     * <p>As with the cluster port, the incarnation must be raised: an older "it is not
     * resting" message may still be propagating, and without the raise it would overwrite
     * this one -- surfacing as "it went to rest, and a moment later was given work again".
     *
     * <p><b>{@code state} is left alone</b>: a resting node is still ALIVE and still takes
     * part in probing and election. Folding this into SWIM's state machine would only get
     * it misjudged as failed.
     *
     * @return the updated node information, or null when nothing changed
     */
    public Node updateSelfOnBreak(boolean resting) {
        lock.lock();
        try {
            Node me = self.get();
            if (me.onBreak() == resting) {
                return null;
            }
            Node updated = me.withBreak(resting, me.incarnation() + 1);
            self.set(updated);
            refreshSnapshots();
            log.info(resting ? "This node is now resting and will not send or receive business messages"
                    : "This node has finished resting and resumes business messaging");
            return updated;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Clears the "holds the cluster port" flag on a node.
     *
     * <p>Used when yielding: with two nodes each holding the cluster port on their own
     * machine, the one later in the order releases its port -- and that fact has to reach
     * the local view immediately, or it would take another gossip round to correct.
     */
    public void clearClusterPortFlag(String nodeId) {
        lock.lock();
        try {
            Entry e = entries.get(nodeId);
            if (e == null || !e.node.clusterPortHolder()) {
                return;
            }
            e.node = e.node.withClusterPort(false, e.node.incarnation() + 1);
            refreshSnapshots();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Recomputes the snapshots and the leader. Must be called while holding the lock.
     *
     * @return whether the leader changed
     */
    private boolean refreshSnapshots() {
        Node me = self.get();
        List<Node> all = new ArrayList<>(entries.size() + 1);
        List<Node> members = new ArrayList<>(entries.size() + 1);

        all.add(me);
        if (me.state().isMember()) {
            members.add(me);
        }
        for (Entry e : entries.values()) {
            all.add(e.node);
            if (e.node.state().isMember()) {
                members.add(e.node);
            }
        }
        // Node is Comparable in its own right, and that comparison is the takeover order
        members.sort(null);

        this.allSnapshot = List.copyOf(all);
        this.membersSnapshot = List.copyOf(members);

        Node newLeader = pickLeader(members);
        Node oldLeader = this.leader;
        this.leader = newLeader;
        return !sameNode(oldLeader, newLeader);
    }

    /**
     * Every member <b>claiming</b> to hold the cluster port, in takeover order.
     *
     * <h2>Normally this has exactly one element</h2>
     * More than one is <b>split brain</b>: several nodes each holding the cluster port on
     * their own host, unaware of one another -- or only just now aware.
     *
     * <p>Note the word claiming. The data comes from the gossiped membership view and
     * reflects <b>what this node can see</b>. During a partition each side sees one leader
     * and neither can detect the split; that is what CAP dictates and no implementation
     * escapes it. Once the two sides can communicate, this list becomes two elements and
     * yielding converges it.
     */
    public List<Node> clusterPortHolders() {
        List<Node> holders = new ArrayList<>(1);
        for (Node n : membersSnapshot) {
            if (n.clusterPortHolder()) {
                holders.add(n);
            }
        }
        return holders;
    }

    private static Node pickLeader(List<Node> sortedMembers) {
        for (Node n : sortedMembers) {
            if (n.clusterPortHolder()) {
                return n;
            }
        }
        // The gap after the old leader departed and before a new one claimed the port:
        // there genuinely is no leader right now
        return null;
    }

    private static boolean sameNode(Node a, Node b) {
        if (a == b) {
            return true;
        }
        return a != null && b != null && a.id().equals(b.id());
    }

    /** An internal entry: the node information plus the locally recorded time of its last state change. */
    private static final class Entry {
        Node node;
        long stateChangeMs;

        Entry(Node node, long stateChangeMs) {
            this.node = node;
            this.stateChangeMs = stateChangeMs;
        }
    }

    /**
     * The result of a merge.
     *
     * @param changes the membership changes produced
     * @param refuted whether a misjudgement about this node prompted a refutation
     */
    public record MergeResult(List<MembershipChange> changes, boolean refuted) {
        public static final MergeResult EMPTY = new MergeResult(List.of(), false);

        public boolean isEmpty() {
            return changes.isEmpty() && !refuted;
        }
    }
}
