package org.riderzen.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
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

    private String name;
    private int port;
    private String bindAddress;
    private int maxThreads;
    private Server server;
    private EventLoop loop;
    private ExecutorService service;

    @Override
    public void configure(Context context) {
        name = "MsgPackSource_" + counter.getAndIncrement();
        port = context.getInteger(PORT);
        bindAddress = context.getString(BIND);
        maxThreads = context.getInteger(THREADS, 1);
        service = Executors.newSingleThreadExecutor();

        logger.info("port: " + port + " bindAddress: " + bindAddress + " maxThreads: " + maxThreads);
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", this);

        Runnable runner = new MsgPackRunnable(this);
        service.submit(runner);

        super.start();
        logger.info("MsgPack source {} was started.", getName());
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping {}...", getName());
        try {
            server.close();
            loop.shutdown();
        } catch (Exception e) {
            logger.error("Can't stop MsgPack source", e);
        }
        super.stop();
        logger.info("MsgPack source was stopped.");
    }

    @Override
    public synchronized String getName() {
        return name;
    }

    @Override
    public synchronized void setName(String name) {
        this.name = name;
    }

    private class MsgPackRunnable implements Runnable {
        private MsgPackSource source;

        public MsgPackRunnable(MsgPackSource source) {
            this.source = source;
        }

        @Override
        public void run() {
            try {
                server = new Server();
                server.serve(new MsgPackServer(source));
                server.listen(bindAddress, port);

                loop = EventLoop.start(Executors.newFixedThreadPool(maxThreads));
                loop.join();
            } catch (Exception e) {
                logger.error("Can't start MsgPack source", e);
            }
        }
    }
}
