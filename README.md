flume-ng-msgpack-source
=============
Flume NG MessagePack source. The source was implemented by MessagePack-RPC.

Download:

https://github.com/leonlee/flume-ng-msgpack-source/raw/master/flume-ng-msgpack-dist-1.0.0.tar.bz2

## Getting Started
- - -
1. Clone the repository
2. Install latest Maven and build source by 'mvn package'
3. Generate classpath by 'mvn dependency:build-classpath'
4. Append classpath in $FLUME_HOME/conf/flume-env.sh
5. Add the source definition according to **Configuration**

## Configuration
- - - 
	type: org.riderzen.flume.source.MsgPackSource
	bind: host name or ip address [0.0.0.0]
	port: port [1985]
	threads: max threads of msgpack Eventloop [1]
	queueSize: the max queue size of blocking message @see java.util.concurrent.ThreadPoolExecutor#ThreadPoolExecutor.

### flume.conf sample
- - -
        agent2.sources = source2
        agent2.channels = channel2
        agent2.sinks = sink2

        agent2.sources.source2.type = org.riderzen.flume.source.MsgPackSource
        agent2.sources.source2.bind = localhost
        agent2.sources.source2.port = 1985

        agent2.sources.source2.channels = channel2

        agent2.sinks.sink2.type = org.riderzen.flume.sink.MongoSink
        agent2.sinks.sink2.host = localhost
        agent2.sinks.sink2.port = 27017
        agent2.sinks.sink2.model = single
        agent2.sinks.sink2.collection = events
        agent2.sinks.sink2.batch = 100

        agent2.sinks.sink2.channel = channel2

        agent2.channels.channel2.type = memory
        agent2.channels.channel2.capacity = 1000000
        agent2.channels.channel2.transactionCapacity = 800
        agent2.channels.channel2.keep-alive = 3
