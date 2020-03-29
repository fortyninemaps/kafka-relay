kafka-consume-srv
=================

Server proxying Kafka consumption.

Use cases:
- fanning out topic consumption across a number of processes greater than the
  partition count
- proxying Kafka to processes without a satisfactory Kafka client library

Clients may send two messages:

1. Read(timeout): requests a Kafka message from one assigned partition within a
   timeout. Server responds with a Message if one is available within the time
   limit.
2. Acknowledge(topic, partition, offset): states that a particular message may
   be committed by the underlying consumer group. Server responds with a 200.
3. Cancel(topic, partition, offset): states that the client has given up on
   consuming a particular message, and it may be consumed again.

The server message response looks like Record(topic, partition, offset, key, value).

When a message is sent in a response to a Read request, it enters a pending
state. A pending message must be acknowledged within a timeout or it becomes
eligible for consumption again.

Multiple servers
----------------

Servers belong to a Kafka consumer group and consume both data topics and server
log topics. Each message within a subscribed topic is consumed by (at least) one
server. When a client requests a Read, it gets a message from one of the
partitions that the active server is assigned to. When a client requests an
Acknowledge, it does not need to send the request to the original server.

Servers write messages that are Pending or Acknowledge to its internal log
topics in Kafka. These are keyed such that the server process responsible for a
partition reads the state of that partition.

Start up
--------

When a server process joins the consumer group, it obtains its assignment to
both the topics to be served (input) and the internal coordination (log) topics.
It reads from the last committed offset in the log topic, building up an
internal representation of which messages are pending and which may be
committed. Once it reaches the end, it begins serving its assigned input topics.

On every Read request, it returns the next eligible (non-pending and
uncommitted) message from the input topics. It records that message as pending.

After an acknowlegement read from the log topic, it checks whether all previous
messages are also acknowledged, in which case it performs a commit of the data
topic.

Committing the log
------------------

Offsets can be committed to the internal coordination topic once it is certain
that
1. all pending messages prior to the offset have completed or expired
2. all acknowleged messages prior to the offset have been committed

The implications of failing to commit to the log are increased catch up time
when new partitions are assigned, because the process needs to read more to
build sufficient state.

Shut down
---------

There isn't anything that needs to happen on shutdown, since nearly all state
can be recovered from the log.

Program architecture
--------------------

There are two parts to the program: the consumer and the server.

When partitions are assigned, the program enters a "catch up" mode and only the
consumer operates. It cannot serve requests until it is caught up on at least
one of the log partitions assigned. As the consumer reads from the log
partitions, it builds up an updated model of the global state (the "state map").
As the consumer reads from the data partitions, it updates or appends to its
state map so that a minimum number of messages are available to be served. The
state map is a CRDT.

When the program is caught up on a log partition, the server becomes active. It
maintains its own copy of the state map, periodically merging in the copy from
the consumer. The server checks its copy of the state map for records that can
be provided in respnse the Read requests. When it chooses one, it marks it as
pending in its copy of the state and also publishes the read to the log. When
the server receives an Acknowledge request, it updates its copy of the state map
and publishes to the log.

The consumer's state map is kept consistent because all modifications are
dual-written to the log, from which it inserts them into its own copy. The
server's state map is kept consistent by periodically merging from the consumer
according to the CRDT rules.

Client consumer groups
----------------------

TODO

