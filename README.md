# spreader

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/maven--central-1.0.0--SNAPSHOT-blue.svg)](https://central.sonatype.com/)

### Your JVM processes are the cluster. Nothing else to install.

**Turn plain JVM instances into a cluster that elects a leader, tracks every member and
passes messages between them. One dependency, one line of configuration, and no ZooKeeper,
etcd, Consul or registry anywhere in the picture.**

```java
GossipCluster cluster = GossipCluster.create(
        GossipConfig.builder().ipAddresses("10.0.0.1", "10.0.0.2").build());
cluster.start();

if (cluster.isLeader()) {
    runTheNightlyJob();          // exactly one instance does this
}
```

---

## Features

| | What it solves |
|---|---|
| **Cluster formation** | A list of addresses, or an address range, is the whole configuration. New machines need no config change at all |
| **Leader election** | Pre-emptive: whichever node holds the cluster port leads. No vote, no term, no quorum, and takeover follows an order every node already agrees on |
| **Member awareness** | Every node holds a complete member list and any two views converge. Join, leave, leader change and death all arrive as callbacks |
| **Failure detection** | Direct probe, then indirect probe through peers, then a suspect timeout, with tombstones so a dead node cannot return as a rumour |
| **Messaging** | Broadcast to all, to one application's replicas, to a single node, or routed by consistent hash on a key. Named channels keep a busy topic from stalling a quiet one |
| **Four transports, one wire format** | Built-in NIO, or Netty, MINA, Grizzly, over TCP or UDP. Nodes running *different* implementations interoperate, re-checked on every change |
| **Bounded queues** | Every dispatch queue is bounded and whatever it drops is counted, so load shedding is never silent |
| **Observable** | Per-channel throughput, latency percentiles, error rates, buffer watermarks. The two failures that log nothing, dropped messages and a second leader, each have a counter |
| **Stated limits** | Under a network partition two sides can each hold a leader. Sub-second recovery, every occurrence counted, and [Limits](#limits) says where that is not good enough |

## Architecture

Two mechanisms, both deliberately boring.

**Membership spreads by SWIM gossip.** Every second each node picks a few peers at random and
exchanges member lists directly, never through a coordinator.

```
    Node A ◄──────── gossip ────────► Node B
       ▲  holds :22000                   ▲
       │  = is the leader                │
       └──────────── gossip ─────────────┘
                      │
                   Node C
```

**Leadership is pre-emptive.** The cluster has one fixed port and whichever node holds it
leads. A new node scans the configured addresses, knocks on that port, and either finds a
leader or becomes one.

```
leader dies
    │
    ├─ failure detection notices (probe, indirect probe, suspect timeout)
    │
    ├─ every eligible node computes its rank in the takeover order
    │
    ├─ rank 0 waits 0ms, rank 1 waits takeoverDelayMs, rank 2 waits 2x ...
    │
    └─ first to bind :22000 is the new leader; the rest see it and stand down
```

### Pre-emptive and consensus-based election are two routes, not two grades

Where a leader's legitimacy comes from is the root of every other difference. Pre-emptive:
from holding a physically exclusive resource. Consensus-based: from a majority's
authorisation, since two majorities must intersect and a node in that intersection will not
vote twice in one term.

| | Pre-emptive (this library) | Consensus-based (Raft) |
|---|---|---|
| How a leader arises | whoever takes the port | whoever a majority votes for |
| Must the leader be up to date? | **No.** A node that just restarted can lead | **Yes.** A stale log cannot win a vote |
| Terms | none, so no built-in fencing | monotonic; an older term is refused |
| One node | works | works |
| Two nodes, one dies | the survivor takes over | **stalls**, no majority |
| Membership changes | soft state, gossiped, free | configuration state, changed through the log |
| Disk | none | `currentTerm` and `votedFor` fsynced before voting |
| Under partition | both sides can lead and accept writes | the minority elects nobody and refuses writes |
| Takeover time | 3 to 4 seconds, dominated by failure detection | typically a few hundred milliseconds |

Two consequences shape how this library should be used:

- **A pre-emptive leader carries no data authority.** It is not required to be up to date, so
  any state it holds has to be rebuildable.
- **With no terms there is no built-in fencing.** A leader already replaced can still write to
  an outside resource. Where that matters, carry a monotonic token from the leader into
  whatever you write and have the resource refuse older ones.

The dividing line is the **nature of the work, not its importance**. Coordination where a
repeat is merely wasteful fits this route. Work where a repeat causes real harm needs
consensus-based election.

## Requirements

| | |
|---|---|
| **Java** | 17 or later |
| **Optional** | Netty, MINA or Grizzly, to replace the built-in NIO transport |
| **Ports** | one cluster port, identical on every node (22000 by default), plus one work port per node (chosen automatically) |

## Quick Start

**Install**

```xml
<dependency>
    <groupId>com.chaconne-ai</groupId>
    <artifactId>spreader</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Snapshots are not in the Central release repository, so declare the repository too:

```xml
<repositories>
    <repository>
        <id>central-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>
```

**Run it**

```bash
java -cp spreader-1.0.0-SNAPSHOT.jar \
     com.chaconneai.spreader.example.BestPractice \
     --cluster=demo --port=22000 --ipAddresses=127.0.0.1
```

Start two or three in separate terminals, adding `--portRetry=10` on one machine.

**Expected output**

```
Node started: cluster=demo, name=demo-app, id=a1b2c3d4, workPort=127.0.0.1:30001,
              clusterPort=22000, transport=TCP/NioTcpTransport
joined: the only node so far, so this one leads

===== cluster [demo] =====
this node: demo-app@127.0.0.1:30001  [leader]
leader:    demo-app@127.0.0.1:30001
members:   1
  - demo-app@127.0.0.1:30001  state=ALIVE  <- me  <- leader

>>> [joined] demo-app@127.0.0.1:30002, members now 2
>>> [joined] demo-app@127.0.0.1:30003, members now 3
```

Kill the leader and watch the takeover:

```
>>> [leader changed] 127.0.0.1:30001 -> 127.0.0.1:30002  (this node is now the leader)
```

**Minimal code**

```java
GossipConfig config = GossipConfig.builder()
        .clusterName("order-cluster")               // the only means of isolation
        .nodeName("order-service")                  // instances sharing this are replicas
        .ipAddresses("10.0.0.11", "10.0.0.12")      // where to look for peers
        .build();

GossipCluster cluster = GossipCluster.create(config);
cluster.start();
cluster.awaitJoin(60, TimeUnit.SECONDS);
```

In a container, add one line: `.advertiseHost(System.getenv("POD_IP"))`. Auto-detection may
pick an address no peer can reach.

## Examples

Two runnable classes in `com.chaconneai.spreader.example`.

### `BestPractice`: forming a cluster, step by step

Each method is a working fragment, and `main()` runs a live node.

| Method | Covers |
|---|---|
| `minimal()` | the smallest cluster that actually works |
| `inContainer(podIp)` | the `advertise-host` trap, where most container deployments stall |
| `waitUntilReady(cluster)` | what to wait for before branching on `isLeader()` |
| `listener()` | every callback, with what belongs inside each |
| `sendingMessages(cluster)` | multicast, targeted multicast, unicast, key-routed unicast |
| `severalApplications(cluster)` | `membersOf`, and why it is the method to reach for |
| `followerOnly()` | an application that joins and works but never leads |
| `doNotDoThis()` | why `isLeader()` is the wrong way to de-duplicate a scheduled task |
| `observability(cluster)` | the three numbers to put on a dashboard |
| `tunedForProduction()` | a full config with the reasoning behind each value |
| `shutdownGracefully(cluster)` | leaving cleanly so peers do not wait out a timeout |

### `ReplicatedH2Example`: a replicated database on the cluster

**Input**: any node writes, no matter which one.

```java
store.write("INSERT INTO product (name, price) VALUES ('widget-1', 10.99)");
```

**Execution**: the write takes one path only.

```
write on a follower          write on the leader
       │                            │
 sendToLeader(sql)                  │
       │                            │
       └────► the leader applies it locally
                      │
             multicast to every replica
                      │
             each applies it to its own H2
```

**Output**: every node reads its own copy, and no read leaves the process.

```java
List<Object[]> rows = store.read("SELECT count(*) FROM product");
```

```bash
java -cp spreader.jar:h2.jar \
     com.chaconneai.spreader.example.ReplicatedH2Example --port=22000
```

```
[127.0.0.1:30001 leader] rows=8 applied=8 members=3
[127.0.0.1:30002]        rows=8 applied=8 members=3
[127.0.0.1:30003]        rows=8 applied=8 members=3
```

It compiles against `java.sql` alone, so H2 is a runtime requirement only and not a
dependency of this library. Four messages make up the protocol: a forwarded write, a
replicated statement, a snapshot request, and a snapshot. That last pair matters more than it
looks, since a node joining an established cluster starts empty.

Its limits are those of asynchronous single leader replication, not of this code: a follower
read can lag by milliseconds, a write is lost if the leader dies between applying and
broadcasting, and a partition lets both sides accept writes that then diverge. It suits
reference data, dictionaries, feature flags and routing tables. Not ledgers or stock counts.

### Building your own distributed component

**Claim a channel, route writes through the leader, replicate the operation, read locally.**

```java
// 1. A private channel. Traffic here cannot stall other components.
cluster.addListener("inventory", (sender, content) -> applyLocally(decode(content)));

// 2. Writes go through the leader, which gives them a total order.
if (cluster.isLeader()) {
    applyLocally(op);
    cluster.multicastOn("inventory", null, encode(op, nextVersion()), false);
} else {
    cluster.sendToLeaderOn("inventory", encode(op));
}

// 3. Reads never leave the process.
public Item read(String id) {
    return localState.get(id);
}
```

| Decision | Why |
|---|---|
| **Replicate the operation, not the state** | `setbit(k, 12345)` costs one datagram whatever the size of the bitmap. Sending the bitmap costs the bitmap |
| **Let the leader assign the order** | Every node applies the same operations in the same sequence, so replicas converge with no merge logic |
| **Number every operation** | A follower receiving version 7 while sitting at 5 knows it missed one and can ask for a snapshot instead of diverging silently |

`ReplicatedH2Example` is this pattern end to end in 350 lines.
[`openspreader`](https://github.com/chaconne-ai/openspreader) is the same pattern applied to a
distributed lock, semaphore, latch, barrier, replicated cache, task distribution and RPC.

## Configuration

Defaults are tuned for a small cluster on a reliable LAN. One line genuinely has to change.

### Essential

| Property | Default | Description |
|---|---|---|
| `ip-addresses` | `127.0.0.1` | Where to look for peers. A few seeds are enough; a new node learns the full member list from any one of them |
| `cluster-name` | `default` | **The only means of isolation.** Nodes with different names ignore each other even when the network can reach them |
| `node-name` | `default` | The application name. Instances sharing one are replicas, and the layers above use it to tell who belongs with whom |
| `cluster-port` | `22000` | The port that decides leadership. Must be identical on every node |
| `advertise-host` | auto-detected | **Set this in containers.** Auto-detection may pick an address no peer can reach |
| `leader-eligible` | `true` | `false` makes this application join and work but never contend for leadership |

### Transport

| Property | Default | Description |
|---|---|---|
| `transport-type` | `TCP` | `TCP` unless measured otherwise |
| `transport-provider` | `NIO` | Built-in, zero extra dependencies. Switch to `NETTY` **only** if you also switch to UDP |
| `direct-buffers` | `false` | Off-heap I/O buffers. The public API hands you `byte[]`, so what off-heap saves inbound it pays back copying out. Set `-XX:MaxDirectMemorySize` if you enable it |

### Payload delivery

| Property | Default | Description |
|---|---|---|
| `payload-ack` | `true` | Wait for an ACK and resend if it does not come. **Off is roughly 3x the throughput**, since the ACK round trip dominates small-message cost |
| `payload-retries` | `2` | Resends reuse the sequence number and the receiver deduplicates on `sender + seq`, so a resend never delivers twice |
| `payload-dedup-ttl-ms` | `60000` | Must outlive the whole retry window |

### Failure detection

| Property | Default | Description |
|---|---|---|
| `gossip-interval-ms` | `1000` | |
| `probe-timeout-ms` | `800` | Raise this first on a congested network. Too low and healthy nodes get suspected during a GC pause |
| `suspect-timeout-ms` | `5000` | Roughly how long until a dead node is declared dead |

### By cluster size

| Nodes | Change |
|---|---|
| < 10 | defaults |
| 10 to 50 | `gossip-fanout=4`, `worker-threads=32` |
| > 50 | `gossip-interval-ms=2000`, `gossip-fanout=5`, consider UDP |

### Who may become the leader

When several applications share one cluster name, some are poor candidates. The leader holds
the lock and permit registers and the authoritative cache copy, so it wants a long lived,
evenly loaded instance. An API facade that scales to zero overnight will win the port just as
readily, and you get a change of leader on every deployment.

```properties
# Joins the cluster and does the work, but never contends for leadership.
# Set per application, not per node.
leader-eligible=false
```

One consequence to know: **the leader is also the rendezvous point for discovery**, since
discovery knocks on the cluster port. Set this on every application and the cluster has no
leader and the members cannot find each other either, each sitting alone with a member list of
one. That is reported as a periodic warning rather than corrected, because electing someone
anyway would override an explicit configuration.

## Performance

Numbers expose a property of the design. **Absolute values will not transfer to your
hardware**: every cross-node call here costs a loopback hop instead of a real network one. The
ratios will.

**Conditions**: single machine over loopback, 8 threads x 2000 synchronous round trips,
~300 byte payloads, JDK serialization, 4-core container.

### Transport x protocol: the pairing is the unit of choice

| Combination | QPS | avg | P50 | P99 | max |
|---|---:|---:|---:|---:|---:|
| **NIO / TCP** | **18,464** | 0.426 ms | 0.367 ms | 1.69 ms | 5.4 ms |
| NIO / UDP | 13,023 | 0.606 ms | 0.423 ms | 6.46 ms | 15.0 ms |
| MINA / TCP | 12,249 | 0.640 ms | 0.438 ms | 5.18 ms | 15.4 ms |
| NETTY / UDP | 10,682 | 0.746 ms | 0.486 ms | 7.19 ms | 28.9 ms |
| MINA / UDP | 10,494 | 0.751 ms | 0.464 ms | 7.24 ms | 21.8 ms |
| GRIZZLY / TCP | 9,171 | 0.864 ms | 0.580 ms | 7.68 ms | 21.3 ms |
| GRIZZLY / UDP | 8,751 | 0.910 ms | 0.644 ms | 6.96 ms | 20.2 ms |
| NETTY / TCP | 7,081 | 1.115 ms | 0.495 ms | 14.46 ms | 56.8 ms |

**Pick the cell, not the framework.** Netty is second-best on UDP and *last* on TCP, with a
P99 twice anyone else's. MINA is the opposite. "Which transport is fastest" has no answer.

Treat anything under ~20% as noise: an earlier run on a 12-core host put Netty/UDP on top, and
NIO/TCP measured 16,351 and 14,637 on two consecutive runs.

### The same four transports, through the components

A lock acquisition is a message to the leader plus a reply; a cache write is a message to the
leader plus a broadcast back. Different shape, different answer.

**TCP**

| Operation | NIO | NETTY | MINA | GRIZZLY |
|---|---:|---:|---:|---:|
| lock acquire + release | **875** | 686 | 616 | 648 |
| semaphore acquire + release | **806** | 655 | 546 | 654 |
| cache write (`incr`) | **2,110** | 1,733 | 1,440 | 1,748 |
| task dispatch to a peer | **11,581** | 10,028 | 11,473 | 9,148 |

**UDP**

| Operation | NIO | NETTY | MINA | GRIZZLY |
|---|---:|---:|---:|---:|
| lock acquire + release | 2,878 | 2,162 | **3,009** | 720 |
| semaphore acquire + release | **3,832** | 1,777 | 1,719 | 709 |
| cache write (`incr`) | **8,326** | 8,237 | 2,645 | 1,618 |
| task dispatch to a peer | 5,809 | **10,787** | 10,407 | 8,856 |

| Observation | Why it matters |
|---|---|
| **Coordination runs 3 to 4x faster over UDP** | Locks 875 to 2,878, cache writes 2,110 to 8,326. These are short one-way messages plus a reply, and TCP's acknowledgement and congestion control are pure overhead on that shape. A sustained request/response stream is the opposite, and TCP wins there |
| **Grizzly collapses on UDP** | 720 against NIO's 2,878 is four-fold, far outside the noise band, and it appears on every coordination row while leaving task dispatch alone |

Reads are absent from both tables on purpose: they come from local memory and never touch the
transport, so comparing them would only mislead.

### Dispatch: more threads is not more throughput

| Mode | msg/s | per message |
|---|---:|---:|
| 4 producers, 4 dispatch threads | 5,334,329 | 187 ns |
| serial dispatch (`payload-dispatch-threads=1`) | 5,100,959 | 196 ns |
| concurrent dispatch (`payload-dispatch-threads=4`) | 3,453,596 | 290 ns |

Rows two and three share a single producer, and **the serial dispatcher wins by almost 50%**.
Coordination between dispatch threads costs more than the parallelism returns.
`payload-dispatch-threads` is not a throughput dial: raise it when several threads publish
concurrently, leave it at 1 otherwise.

### What these numbers say about the design

| | |
|---|---|
| **Cost is the round trip, not serialisation** | JDK and Kryo differ by ~5% once the network is in the path (4,462 vs 4,226 QPS), against a ~20% gap in pure encode/decode |
| **ACK is the throughput knob that matters** | Turning `payload-ack` off moves unicast from ~1,900 to ~20,600 msg/s, an 11x difference |
| **Connection reuse decides whether TCP survives** | With pooling off, 100 unicasts leave **111 sockets in TIME_WAIT**; with it on, zero. A short benchmark with pooling off looks healthy right until the socket table fills |

## Observability

```java
ChannelMetrics m = cluster.metrics("audit");
m.tps(); m.errorRate(); m.outboundLatency().p99Nanos();

List<BufferMetrics> buffers = cluster.bufferMetrics();
SplitBrainStatus split = cluster.splitBrainStatus();
```

Two numbers deserve alerts, because **neither produces a log line**:

| Signal | Meaning |
|---|---|
| `BufferMetrics.dropped() > 0` | Messages were thrown away under load. Sender does not know, receiver does not know |
| `SplitBrainStatus.occurrences()` | How many times this node found another cluster-port holder. Non-zero is normal after a simultaneous restart; climbing steadily is not |

With Spring Boot, [`openspreader`](https://github.com/chaconne-ai/openspreader) exposes all of
it on `/actuator/prometheus` plus a human-readable `/actuator/spreader-report`.

## How This Is Verified

| Layer | What it covers |
|---|---|
| **In-JVM suite** | Multi-node clusters in one JVM with real sockets and real serialisation, across 4 transports x 2 protocols x 2 serializations. All four speak the same wire format, and "still interoperates" is re-checked rather than assumed |
| **Mainline, 8 cells** | TCP/UDP x NIO/NETTY x JDK/KRYO, 652 cases per cell, green as of 2026-10-01 |
| **Full matrix, 16 cells** | Adds the transport layer and process lifecycle suites |
| **Cross-container** | Three containers on a fixed-IP subnet, started simultaneously so they genuinely race for the cluster port |

TCP finishes the same cases about 40% faster than UDP (145s vs 280s): UDP pays for fragment
reassembly and idempotent de-duplication that TCP hands to the kernel.

Fixed IPs rather than service names are deliberate, because Docker's DNS would paper over a
misconfigured `advertise-host`, the single most common way a containerised cluster fails to
form. That layer earned its place by catching a bug every in-JVM test missed: on one machine
the kernel refuses a second bind, but across machines each host binds its own 22000 with no
conflict, and the discovery path had quietly inherited the single-machine assumption. Three
containers started together produced three isolated single-node clusters that never merged.

## Limits

| Limit | Detail |
|---|---|
| **Election is pre-emptive, not consensus-based** | Under partition two sides can each elect a leader. It heals when the partition does, and every occurrence is counted. Where a repeat causes real harm, use consensus-based election, or carry a fencing token |
| **Membership is eventually consistent** | Right after a change, different nodes briefly hold different views |
| **UDP messages can be lost** | `payload-ack` covers it with resend and dedup, at a throughput cost |
| **Cross-machine performance is unmeasured** | Every number above is one JVM over loopback |

## Spring Boot

[**openspreader**](https://github.com/chaconne-ai/openspreader) is the starter built on this:
distributed locks, semaphores, latches, barriers, a replicated cache, cluster scheduling, RPC,
MapReduce and DAG orchestration, with auto-configuration and actuator endpoints.

## Contributing

| Rule | Why |
|---|---|
| **Every fix needs a regression test** | Coverage is not a target; a bug coming back is the line |
| **Run the full suite, not the single case** | "Green alone, red in the full run" has been a real defect every time it has come up here |
| **Comments are written in English**, and explain *why* rather than restating *what* | The reasoning behind a non-obvious decision is the part worth writing down |
| **Nothing may be added to the runtime dependencies** | A design constraint, not an accident. An optional dependency, guarded so the library works without it, is a different matter |

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
Copyright 2026 [ChaconneAI](https://github.com/chaconne-ai).
