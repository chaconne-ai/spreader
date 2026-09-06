# spreader

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/maven--central-1.0.0--SNAPSHOT-blue.svg)](https://central.sonatype.com/)

**Decentralized clustering for Java: instances form a cluster in seconds, elect
a leader, keep a live view of every member, and multicast or unicast messages to
one another.**

Every node runs the same code and holds the same complete member list.
Membership spreads by SWIM gossip, point to point between randomly chosen peers
and never through a coordinator, so no node exists whose loss stops the rest. A
node that dies is found by direct probe, then by indirect probe through other
members, then by a suspect timeout. Leadership is decided by seizing the cluster
port: the operating system already guarantees that only one process can hold a
port, so there is no election round, no term and no quorum.

Messaging travels the same channel as membership. Broadcast to the whole
cluster, to the instances of one application, to a single node, or routed by
consistent hash on a key. Measured on the built-in transport over TCP: 18,464
cross-node request/response round trips per second, and 5.3 million in-process
dispatches per second.

```java
GossipCluster cluster = GossipCluster.create(
        GossipConfig.builder().ipAddresses("10.0.0.1", "10.0.0.2").build());
cluster.start();

if (cluster.isLeader()) {
    runTheNightlyJob();          // exactly one instance does this
}
```

## What you get

| | |
|---|---|
| **Cluster formation** | A list of addresses, or an address range, is the whole configuration. A node scans it on startup and either joins whoever answers on the cluster port or becomes the first member. Give it a range and new machines need no config change at all. |
| **Leader election** | Whoever seizes the cluster port is the leader. No consensus round, no term, no quorum, no replicated log. Takeover follows a fixed order, so the death of a leader promotes a successor that every node already agrees on. |
| **Member awareness** | SWIM gossip between randomly chosen peers. Every node holds a complete member list and any two views converge. Join, leave, leader change and node death all arrive as callbacks. |
| **Failure detection** | Direct probe, then indirect probe through other members, then a suspect timeout, with tombstones so a node that died does not come back as a rumour. |
| **Decentralized throughout** | No coordinator, no registry, no external state, and no node the others depend on. Every node runs identical code, and killing any one of them leaves a working cluster behind. |
| **Multicast and unicast** | To the whole cluster, to the instances of one application, to a single node, or routed by consistent hash on a key. Round-robin, random and weighted balancing as well, choosable per call. Independent named channels keep a busy topic from stalling a quiet one. |
| **Performance** | 18,464 cross-node round trips per second and 5.3 million in-process dispatches per second, on the built-in NIO transport over TCP. The full matrix, and the conditions behind it, are under [Performance](#performance-and-what-it-tells-you-about-the-design). |
| **Four transports, one wire format** | The built-in NIO one, or Netty, MINA, Grizzly, over TCP or UDP. Nodes running *different* implementations interoperate in the same cluster, and that is re-checked on every change rather than assumed. |
| **Bounded and observable** | Every dispatch queue is bounded and whatever it drops is counted. Per-channel throughput, concurrency, latency percentiles, error rates and buffer watermarks are readable at runtime, and the two failures that produce no log line, dropped messages and a second leader, each have a counter. |
| **Stated limits** | This is not Raft. Under a network partition two sides can each hold a leader. Recovery is sub-second and every occurrence is counted, and [Limits](#limits) says where that is not good enough. |

## Is this the right tool?

| Your situation | |
|---|---|
| 3 to 50 instances of a service that need a leader, a member list, or messages between them | **Yes. This is the case it was built for** |
| You are on Spring Boot and want locks, a cache, cluster scheduling or RPC as well | Use [**openspreader**](../spreader-commons), the starter built on this |
| "This must never run twice, **ever**": money moves, or a ledger is written | **No. Use Raft.** [Limits](#limits) says exactly why |
| Hundreds of nodes, or a cluster spanning datacentres | No. The design and the defaults target a small cluster on a reliable LAN |
| You already operate ZooKeeper or Consul for other reasons | Probably not worth the swap. The operational cost you would save is already being paid |

## Table of contents

- [What you get](#what-you-get)
- [Is this the right tool?](#is-this-the-right-tool)
- [Why](#why)
- [How it works](#how-it-works)
- [Installation](#installation)
- [Quick start](#quick-start)
  - [Running in containers](#running-in-containers)
- [Performance](#performance-and-what-it-tells-you-about-the-design)
- [Recommended configuration](#recommended-configuration)
- [Observability](#observability)
- [Examples](#examples)
- [How this is verified](#how-this-is-verified)
- [Limits](#limits)
- [Spring Boot integration](#spring-boot-integration)
- [Contributing](#contributing)
- [License](#license)

## Why

You have three instances of a service. You need one of them, and only one, to
run the nightly job. Or you need instance A to tell instances B and C that a
cache entry went stale.

The usual answer is ZooKeeper, etcd, or Consul: a separate cluster to install,
monitor, upgrade, and page someone about at 3am. For a service that has three
instances, the coordinator is now the biggest operational item you own.

spreader takes the opposite approach. **The processes coordinate among
themselves.** No coordinator, no registry, no external state.

## How it works

Two mechanisms, both deliberately boring.

**Membership is SWIM gossip.** Every second each node picks a few peers at
random and exchanges member lists directly, point to point, never through a
leader. Failures are detected by direct probe, then indirect probe through
other members, then a suspect timeout. Every node holds a complete member list
and any two views converge.

**Leadership is a port.** Whoever seizes the cluster port (22000 by default) is
the leader. The operating system already guarantees that only one process can
hold a port, so there is no election, no term, no quorum, no consensus round.
A new node scans the configured addresses, knocks on 22000, and either finds a
leader or becomes one.

That second decision is worth being explicit about, because it defines the
system's limits:

- **On one machine it is exact.** The OS refuses the second bind. Period.
- **Across machines it is a race.** Every machine can bind *its own* 22000
  without conflict. Mutual exclusion comes from "scan on startup, plus timing",
  not from the kernel. Two nodes starting simultaneously **can** both become
  leader for a moment.

The brief split brain is expected, not a bug. What matters is that it heals:
nodes that discover another holder whose takeover order comes first will
release the port and join. Recovery is typically well under a second, and every
occurrence is counted in `splitBrainOccurrences` so it is never silent.

**If you need a leader that is correct under network partition, use Raft.**
spreader trades that guarantee for having nothing to operate. For "run this job
on one instance" it is the right trade. For "this must never run twice, ever" it
is not.

## Installation

**Maven**

```xml
<dependency>
    <groupId>com.chaconne-ai</groupId>
    <artifactId>spreader</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Gradle**

```groovy
implementation 'com.chaconne-ai:spreader:1.0.0-SNAPSHOT'
```

### Requirements

| | |
|---|---|
| **Java** | 17 or later |
| **Runtime dependencies** | `slf4j-api` only, a logging facade that pulls in nothing itself |
| **Optional** | Netty, MINA or Grizzly, if you want one of them instead of the built-in NIO transport |
| **Ports** | one cluster port, identical on every node (22000 by default), plus one work port per node (chosen automatically) |


## Quick start

```java
GossipCluster cluster = GossipCluster.create(GossipConfig.builder()
        .clusterName("order-cluster")
        .clusterPort(22000)
        // Where to look for the cluster. Hosts only; the port is the cluster port.
        .ipAddresses("192.168.0.111", "192.168.0.63")
        // Or a range, so new machines need no config change
        .ipAddressRange("192.168.0.1-192.168.5.254")
        .build());

cluster.addListener(new GossipListener() {
    @Override public void onNodeJoined(Node node) { }
    @Override public void onNodeLeft(Node node, boolean graceful) { }
    @Override public void onLeaderChanged(Node prev, Node cur, boolean selfIsLeader) { }
});

cluster.start();
cluster.awaitLeader(30, TimeUnit.SECONDS);

if (cluster.isLeader()) {
    runTheNightlyJob();
}
```

Messaging:

```java
cluster.multicast("cache-invalidated:user:42".getBytes(UTF_8));   // everyone
cluster.multicastTo("report-service", payload);                   // one application
cluster.unicast(payload);                                         // one node, load balanced
cluster.unicast(userId, payload);                                 // consistent hash on a key
```

Separate channels so a busy topic cannot stall a quiet one:

```java
cluster.addListener("audit", auditListener);
cluster.multicastOn("audit", null, payload);
```

### Running in containers

One setting causes most container deployments to fail, and it fails **silently**:
`advertise-host` must be an address other machines can actually dial. Binding
`0.0.0.0` is correct, but if the *advertised* address is `127.0.0.1` or a
container-internal IP, peers never connect. Nodes start, logs look clean, and
they simply never see each other.

```yaml
services:
  node-1:
    environment:
      SPRING_SPREADER_PORT: "22000"            # identical on every node
      SPRING_SPREADER_IPADDRESSES: "172.31.0.11,172.31.0.12,172.31.0.13"
      ADVERTISE_HOST: "172.31.0.11"            # this container's reachable IP
    networks:
      cluster:
        ipv4_address: 172.31.0.11

networks:
  cluster:
    ipam:
      config:
        - subnet: 172.31.0.0/16
```

Fixed IPs rather than service names, deliberately. Docker's DNS resolves service
names and would mask a wrong `advertise-host`, which is precisely the failure
you want your staging environment to surface, not hide.

Expect a **brief split brain** when containers start together: each host binds
its own 22000 without conflict, so more than one node can win momentarily. It
converges in well under a second; `splitBrainOccurrences` records that it
happened.

## Performance, and what it tells you about the design

Numbers here are not a leaderboard. Each one exposes a property of the design
that is worth knowing before you build on it.

Measured on a single machine over loopback: 8 threads × 2000 synchronous round
trips, ~300 byte payloads. **Absolute values will not transfer to your
hardware**: every cross-node call costs a loopback hop instead of a real
network one. The ratios will.

### Transport × protocol: the pairing is the unit of choice

8 threads × 2000 synchronous round trips, ~300 byte payloads, JDK serialization,
inside a 4-core container:

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

The built-in NIO transport on TCP leads on every measure at once: throughput,
median, tail. That combination is the default for a reason.

**Pick the cell, not the framework.** Netty is second-best on UDP and *last* on
TCP, with a P99 twice anyone else's. MINA is the opposite: strong on TCP,
middling on UDP. Asking "which transport is fastest" has no answer; asking
"which transport is fastest over TCP" does.

A word on reading these numbers honestly: an earlier single run on a 12-core
host put Netty/UDP on top at 14,753 QPS, and NIO/TCP measured 16,351 and 14,637
in two consecutive runs. Rankings that swing with the host and the run are not
rankings. **Treat anything under ~20% as noise and benchmark the exact cell you
intend to deploy**. That is the only number that will hold.

### Dispatch: more threads is not more throughput

| Mode | msg/s | per message |
|---|---:|---:|
| 4 producers, 4 dispatch threads | 5,334,329 | 187 ns |
| serial dispatch (`payload-dispatch-threads=1`) | 5,100,959 | 196 ns |
| concurrent dispatch (`payload-dispatch-threads=4`) | 3,453,596 | 290 ns |

Rows two and three share a single producer, and **the serial dispatcher wins by
almost 50%**. Coordination between dispatch threads costs more than the
parallelism returns. Only when the producing side is concurrent too (row one)
does the multi-threaded path pull ahead, and then only barely.

The practical rule: `payload-dispatch-threads` is not a throughput dial. Raise
it when several threads publish concurrently, leave it at 1 otherwise.

### What these numbers say about the design

Three properties, each visible in the measurements above:

**Messaging cost is dominated by the round trip, not by serialisation.** JDK and
Kryo differ by ~5% once the network is in the path (4,462 vs 4,226 QPS), against
a ~20% gap in pure encode/decode. Optimising serialisation is rarely where the
win is.

**ACK is the throughput knob that matters.** Turning `payload-ack` off moves
unicast from ~1,900 to ~20,600 msg/s, an 11× difference, because the
acknowledgement round trip dominates small-message cost. That is the setting to
reach for, not thread counts.

**Connection reuse decides whether TCP survives, not how fast it is.** With
pooling disabled, 100 unicasts leave **111 sockets in TIME_WAIT**; with it
enabled, zero. Throughput improves modestly; sustainability improves
categorically. A short benchmark with pooling off looks healthy right until the
socket table fills.

## Recommended configuration

Defaults are tuned for a small cluster on a reliable LAN. Change these when your
situation differs.

### Start here

```properties
# The port that decides leadership. Must be identical on every node.
cluster-port=22000
# Where to look for peers
ip-addresses=10.0.1.10,10.0.1.11,10.0.1.12
# In containers: the address peers can actually dial
advertise-host=10.0.1.10
```

### Transport

```properties
transport-type=TCP           # TCP unless you have measured otherwise
transport-provider=NIO       # built-in, zero extra dependencies
direct-buffers=false         # off-heap I/O buffers; off by default
```

TCP with the built-in NIO transport is the default for a reason: first-tier
throughput, the lowest worst-case latency in the matrix, and nothing to add to
your dependency tree. Switch to `NETTY` **only** if you also switch to UDP.
That is the pairing where it wins.

UDP is worth it when you have many nodes and mostly one-way traffic: no
handshake, no connection state, no TIME_WAIT. It costs you delivery guarantees,
which is why `payload-ack` exists.

`direct-buffers` switches I/O buffers off-heap. It stays off by default because
the public API hands you `byte[]`: whatever the off-heap path saves on the way
in, it pays back copying to the heap on the way out, and for small messages
off-heap allocation costs more than heap allocation to begin with. It is worth
measuring when your messages are large and frequent. Set
`-XX:MaxDirectMemorySize` if you enable it; off-heap memory is outside the heap
limit, so a leak never triggers GC, it just gets the process killed.

### Payload delivery

```properties
payload-ack=true             # wait for an ACK, resend if it does not come
payload-retries=2
payload-dedup-ttl-ms=60000   # must outlive the whole retry window
```

`payload-ack=false` switches to a one-way message type that the peer never
answers. Roughly **3× the throughput**, because the ACK round trip dominates
small-message cost. Use it for telemetry and cache invalidations where losing
one message is survivable. Keep it on for anything a human would notice.

Resends reuse the same sequence number and the receiver deduplicates on
`sender + seq`, so **a resend never delivers twice** to your listener.

### Failure detection

```properties
gossip-interval-ms=1000
probe-timeout-ms=800
suspect-timeout-ms=5000      # ≈ how long until a dead node is declared dead
```

On a congested network or across regions, raise `probe-timeout-ms` first. Too
low and healthy nodes get suspected during a GC pause, which causes far more
churn than slow detection would.

### Tuning by cluster size

| Nodes | Change |
|---|---|
| < 10 | defaults |
| 10–50 | `gossip-fanout=4`, `worker-threads=32` |
| > 50 | `gossip-interval-ms=2000`, `gossip-fanout=5`; consider UDP |

Gossip traffic grows with fanout × nodes. Beyond ~50 nodes, slowing the interval
costs little in detection time and saves a lot of chatter.

### High throughput

```properties
connection-pool-enabled=true   # on by default; TCP only
pool-max-idle-per-host=8
payload-concurrency=16
payload-dispatch-threads=4     # only if producers are concurrent, see above
```

The connection pool is what keeps TCP usable at rate. Without it every message
opens a connection and leaves a TIME_WAIT: measured **111 TIME_WAIT sockets
after 100 unicasts** with the pool off, **zero** with it on. Its real value is
sustainability rather than peak throughput: a short benchmark with the pool
disabled looks fine right up until the socket table fills.

## Observability

```java
ChannelMetrics m = cluster.metrics("audit");
m.tps(); m.errorRate(); m.outboundLatency().p99Nanos();

List<BufferMetrics> buffers = cluster.bufferMetrics();
SplitBrainStatus split = cluster.splitBrainStatus();
```

Two numbers deserve alerts, because **neither produces a log line**:

- `BufferMetrics.dropped() > 0`: messages were thrown away under load. Sender
  does not know, receiver does not know.
- `SplitBrainStatus.occurrences()`: how many times this node found another
  cluster-port holder. Non-zero is normal after a simultaneous restart;
  climbing steadily is not.

If you use Spring Boot, the [`openspreader`](../spreader-commons) starter exposes
all of this on `/actuator/prometheus` and a human-readable
`/actuator/spreader-report`.

## Examples

Two runnable classes in `com.chaconneai.spreader.example`:

**`BestPractice`**: forming a cluster, step by step. Each method is a working
fragment you can lift:

| Method | Covers |
|---|---|
| `minimal()` | the smallest cluster that actually works |
| `inContainer(podIp)` | the `advertise-host` trap, which is where most container deployments stall |
| `waitUntilReady(cluster)` | what to wait for before branching on `isLeader()` |
| `listener()` | every callback, with what you should and should not do inside each |
| `sendingMessages(cluster)` | multicast, targeted multicast, unicast, key-routed unicast |
| `tunedForProduction()` | a full config with the reasoning behind each value |
| `shutdownGracefully(cluster)` | leaving cleanly so peers do not wait out a timeout |

`main()` runs the whole sequence, so you can watch the real shape of a cluster
coming up.

**`GossipDemo`**: a command-line node. Start several in separate terminals and
watch them find each other, elect a leader, and notice when you kill one:

```bash
java -cp spreader.jar com.chaconneai.spreader.example.GossipDemo \
     --cluster demo --port 22000 --peers 127.0.0.1
```

### Building your own distributed component

The pattern for putting your own logic on top of spreader is: **claim a channel,
route through the leader, replicate the operation.**

```java
// 1. A private channel. Traffic here cannot stall other components.
cluster.addListener("inventory", new GossipListener() {
    @Override
    public void onPayload(Node sender, byte[] content) {
        applyLocally(decode(content));   // every node applies the same operation
    }
});

// 2. Writes go through the leader, which gives them a total order.
if (cluster.isLeader()) {
    long version = nextVersion();
    applyLocally(op);
    cluster.multicastOn("inventory", null, encode(op, version));
} else {
    cluster.unicast(cluster.leader(), encode(op));   // forward, let the leader sequence it
}

// 3. Reads never leave the process.
public Item read(String id) {
    return localState.get(id);
}
```

Three decisions make this work, and all three are worth copying:

**Replicate the operation, not the state.** Sending `setbit(k, 12345)` costs one
datagram no matter how large the bitmap is. Sending the bitmap costs the bitmap.

**Let the leader assign the order.** Every node applies the same operations in
the same sequence, so replicas converge without any merge logic. Followers
forward writes rather than applying them locally.

**Number every operation.** A follower that receives version 7 while sitting at
version 5 knows it missed one, and can ask for a full snapshot instead of
silently diverging. Gaps are inevitable on UDP; noticing them is what keeps
replicas honest.

The `openspreader` starter is this pattern applied seven times: distributed
lock, semaphore, latch, barrier, replicated cache, task distribution, RPC. If
you want a worked reference rather than a sketch, read
`com.chaconneai.openspreader.cache.CacheService`.

## How this is verified

**In-JVM suite: 642 cases per cell, 16 cells.** Multi-node clusters in one JVM,
run across the full matrix of 4 transport implementations × 2 protocols × 2
serializations. All four speak the same wire format, and "still interoperates"
is a claim that has to be re-checked on every change, not assumed.

TCP finishes the same 642 cases about 40% faster than UDP (145s vs 233s).
UDP pays for fragment reassembly and idempotent de-duplication that TCP hands
to the kernel.

**Cross-container clusters.** Three containers on a fixed-IP subnet, started
simultaneously so they genuinely race for the cluster port. Fixed IPs rather
than service names on purpose: Docker's DNS would quietly paper over a
misconfigured `advertise-host`, and that misconfiguration is the single most
common way a containerised cluster fails to form.

This layer earned its place. Leadership rests on holding the cluster port, and
on one machine that *is* mutual exclusion: the kernel refuses the second bind.
Across machines each host binds its own 22000 with no conflict at all, and the
discovery path had quietly inherited the single-machine assumption: a node
holding the port concluded it was the leader and stopped looking for others.
Three containers started together produced three isolated single-node clusters
that never merged. Every in-JVM test passed throughout.

The fix makes an isolated node keep scanning even while holding the port, and
yield to whoever ranks first. Recovery now happens faster than a 0.5s sampling
loop can catch, visible only in the `splitBrainOccurrences` counter, which is
exactly why that counter exists.

## Limits

Worth knowing before you adopt it:

- **Leadership is not consensus.** Under partition, two sides can each elect a
  leader. It heals when the partition does. If double execution is unacceptable,
  use Raft.
- **Membership is eventually consistent.** Right after a change, different nodes
  briefly hold different views.
- **UDP messages can be lost.** `payload-ack` covers it with resend and dedup,
  at a throughput cost.
- **Cross-machine performance is unmeasured.** Every number above is one JVM
  over loopback. Real networks add real round trips.

## Spring Boot integration

If you are on Spring Boot, you almost certainly want
[**openspreader**](../spreader-commons) instead of using this library directly.
It auto-configures the cluster from `application.properties` and adds
distributed locks, semaphores, latches, barriers, scheduled-task exclusion,
a cluster cache, RPC and MapReduce on top of it.

```xml
<dependency>
    <groupId>com.chaconne-ai</groupId>
    <artifactId>openspreader</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

This library remains the right choice when you are not on Spring, or when you
want the cluster and nothing else.

## Contributing

Issues and pull requests are welcome at
[github.com/chaconne-ai/spreader](https://github.com/chaconne-ai/spreader).
For anything that does not belong in a public issue, write to
hello@chaconne-ai.com.

A few things worth knowing before you open one:

- **Every fix needs a regression test.** The test should fail before the fix and
  pass after it. Coverage as a number is not a goal; a bug recurring is the line
  that must not be crossed.
- **Run the full suite, not the single case.** "Green alone, red in the full
  run" has been a real defect every time it has come up in this project, never
  an environment quirk.
- **Comments are written in English**, and they explain *why* rather than
  restating *what*. The reasoning behind a non-obvious decision is the part
  worth writing down.
- **Nothing may be added to the runtime dependencies.** `slf4j-api` alone is a
  design constraint, not an accident. An optional dependency, guarded so the
  library works without it, is a different matter.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
Copyright 2026 [ChaconneAI](https://github.com/chaconne-ai).

```
Copyright 2026 ChaconneAI

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
