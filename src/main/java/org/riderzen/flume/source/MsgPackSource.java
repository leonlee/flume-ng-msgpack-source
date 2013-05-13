package org.riderzen.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 下午4:15
 */
public class MsgPackSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MsgPackSource.class);

    private static AtomicInteger counter = new AtomicInteger();

    public static final String PORT = "port";
    public static final String BIND = "bind";
    public static final String THREADS = "threads";
    public static final String NAME_PREFIX = "MsgPackSource_";
    public static final String QUEUE_SIZE = "queueSize";
    public static final String POOL_SIZE = "poolSize";

    public static final int DEFAULT_PORT = 1985;
    public static final String DEFAULT_BIND = "0.0.0.0";
    public static final int DEFAULT_THREADS = 1;
    public static final int DEFAULT_QUEUE_SIZE = 3g000;
    public static final int DEFAULT_POOL_SIZE = 500;

    private int port;
    private String bindAddress;
    private int maxThreads;
    private int queueSize;
    private int poolSize;

    private Server server;
    private EventLoop loop;

    @Override
    public void configure(Context context) {
        setName(NAME_PREFIX + counter.getAndIncrement());
        bindAddress = context.getString(BIND, DEFAULT_BIND);
        port = context.getInteger(PORT, DEFAULT_PORT);
        maxThreads = context.getInteger(THREADS, DEFAULT_THREADS);
        poolSize = context.getInteger((POOL_SIZE), DEFAULT_POOL_SIZE);
        queueSize = context.getInteger(QUEUE_SIZE, DEFAULT_QUEUE_SIZE);

        logger.info("port: " + port + " bindAddress: " + bindAddress + " maxThreads: " + maxThreads);
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);

        try {
            server = new Server();
            server.serve(new MsgPackServer(this));
            server.listen(bindAddress, port);

            loop = EventLoop.start(Executors.newFixedThreadPool(maxThreads));
        } catch (Exception e) {
            logger.error("Can't start MsgPack source", e);

            stop();
            return;
        }

        super.start();
        logger.info("MsgPack source {} was started.", getName());
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping {}...", getName());
        try {
            if (server != null) {
                server.close();
            }

            if (loop != null) {
                loop.shutdown();
            }
        } catch (Exception e) {
            logger.error("Can't stop MsgPack source", e);
        }
        super.stop();
        logger.info("MsgPack source was stopped.");
    }

    public int getPort() {
        return port;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getPoolSize() {
        return poolSize;
    }
}
