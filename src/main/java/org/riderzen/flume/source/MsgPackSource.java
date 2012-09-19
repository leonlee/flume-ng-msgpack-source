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
    public static final String DECODE = "decode";
    public static final String NAME_PREFIX = "MsgPackSource_";

    private int port;
    private String bindAddress;
    private int maxThreads;

    private Server server;
    private EventLoop loop;

    @Override
    public void configure(Context context) {
        setName(NAME_PREFIX + counter.getAndIncrement());
        port = context.getInteger(PORT);
        bindAddress = context.getString(BIND);
        maxThreads = context.getInteger(THREADS, 1);

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
}
