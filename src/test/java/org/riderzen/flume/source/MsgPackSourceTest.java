package org.riderzen.flume.source;

import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.json.simple.JSONObject;
import org.msgpack.MessagePack;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.tMap;

/**
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 下午5:34
 */
public class MsgPackSourceTest {
    private static final Logger logger = LoggerFactory.getLogger(MsgPackSourceTest.class);
    private MsgPackSource source;
    private Channel channel;

    public void startSource(int portNum) throws InterruptedException {
        source = new MsgPackSource();
        channel = new MemoryChannel();

        channel.setName("Channel" + portNum);

        Context channelCtx = new Context();
        channelCtx.put("capacity", "100000");
        channelCtx.put("transactionCapacity", "100000");
        channelCtx.put("keep-alive", "100000");

        Configurables.configure(channel, channelCtx);

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector selector = new ReplicatingChannelSelector();
        selector.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(selector));

        Context ctx = new Context();

        ctx.put(MsgPackSource.PORT, String.valueOf(portNum));
        ctx.put(MsgPackSource.BIND, "0.0.0.0");
        ctx.put(MsgPackSource.THREADS, String.valueOf(10));

        Configurables.configure(source, ctx);

        source.start();

    }

    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void sourceTest() throws InterruptedException {
        startSource(41414);
        EventLoop cloop = EventLoop.defaultEventLoop();
        Client cli = null;
        MessagePack msgPack = new MessagePack();
        try {
            cli = new Client("localhost", 41414, cloop);

            IMsgPackSource iface = cli.proxy(IMsgPackSource.class);
            String msg = "hello flume";
            JSONObject headers = new JSONObject();
            headers.put("collection", "test");
            headers.put("lenght", 222);

            byte[] bytes = msgPack.write(msg, TString);
            iface.sendMessage(msgPack.write(headers.toJSONString(), TString), bytes);

            Transaction transaction = channel.getTransaction();
            transaction.begin();

            Event event = channel.take();
//            while ((event = channel.take()) != null) {
            String result = new String(event.getBody());
            Assert.assertEquals(msg, result);
            logger.info("taken msg: {}", result);
//            }

            transaction.commit();
            transaction.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            source.stop();
        }
    }

    @Test(groups = {"unit"})
    public void sourceWithDecodeTest() throws InterruptedException {
        startSource(41418);
        EventLoop cloop = EventLoop.defaultEventLoop();
        Client cli = null;
        MessagePack msgPack = new MessagePack();
        try {
            cli = new Client("localhost", 41418, cloop);

            IMsgPackSource iface = cli.proxy(IMsgPackSource.class);
            String msg = "hello flume";
            JSONObject headers = new JSONObject();
            headers.put("collection", "test");
            headers.put("lenght", 222);

            byte[] bytes = msgPack.write(msg, TString);
            iface.sendMessage(msgPack.write(headers.toJSONString(), TString), bytes);

            Transaction transaction = channel.getTransaction();
            transaction.begin();

            Event event = channel.take();
//            while ((event = channel.take()) != null) {
            String result = new String(event.getBody());
            Assert.assertEquals(msg, result);
            logger.info("taken msg: {}", result);
//            }

            transaction.commit();
            transaction.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            source.stop();
        }
    }

    @Test(groups = {"manu"})
    public void sourceServerTest() throws InterruptedException {
        startSource(1985);

        Transaction tx = channel.getTransaction();
        tx.begin();

        Event event = null;
        while ((event = channel.take()) != null) {
            logger.debug("=========================");
            Map<String, String> headers = event.getHeaders();
            for (String name : headers.keySet()) {
                logger.debug("{}:{}", name, headers.get(name));
            }
            String message = new String(event.getBody());
            logger.error("{}", message);
            logger.debug("=========================");
        }
    }
}
