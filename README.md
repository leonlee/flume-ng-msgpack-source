flume-ng-msgpack-source
=============
Flume NG MessagePack source. The source was implemented by MessagePack-RPC.

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
	bind: host name or ip address, '0.0.0.0'
	port: port, 1985 
