package org.riderzen.flume;

import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.loop.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.msgpack.template.Templates.TString;

/**
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 下午5:34
 */
public class MsgPackSourceTest {
    private static final Logger logger = LoggerFactory.getLogger(MsgPackSourceTest.class);
    private MsgPackSource source;
    private Channel channel;

    public void startSource(Boolean needDecode, int portNum) throws InterruptedException {
        source = new MsgPackSource();
        channel = new MemoryChannel();

        channel.setName("Channel" + portNum);

        Context channelCtx = new Context();
        Configurables.configure(channel, channelCtx);

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector selector = new ReplicatingChannelSelector();
        selector.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(selector));

        Context ctx = new Context();

        ctx.put(MsgPackSource.PORT, String.valueOf(portNum));
        ctx.put(MsgPackSource.BIND, "0.0.0.0");
        ctx.put(MsgPackSource.DECODE, needDecode.toString());
        ctx.put(MsgPackSource.THREADS, String.valueOf(10));

        Configurables.configure(source, ctx);

        source.start();

    }

    @Test
    public void sourceTest() throws InterruptedException {
        startSource(Boolean.FALSE, 41414);
        EventLoop cloop = EventLoop.defaultEventLoop();
        Client cli = null;
        MessagePack msgPack = new MessagePack();
        try {
            cli = new Client("localhost", 41414, cloop);

            IMsgPackSource iface = cli.proxy(IMsgPackSource.class);
            String msg = "hello flume";
            byte[] bytes = msgPack.write(msg, TString);
            iface.sendMessage(bytes);

            Transaction transaction = channel.getTransaction();
            transaction.begin();

            Event event = channel.take();
//            while ((event = channel.take()) != null) {
            String result = msgPack.read(event.getBody(), TString);
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

    @Test
    public void sourceWithDecodeTest() throws InterruptedException {
        startSource(Boolean.TRUE, 41418);
        EventLoop cloop = EventLoop.defaultEventLoop();
        Client cli = null;
        MessagePack msgPack = new MessagePack();
        try {
            cli = new Client("localhost", 41418, cloop);

            IMsgPackSource iface = cli.proxy(IMsgPackSource.class);
            String msg = "hello flume";
            byte[] bytes = msgPack.write(msg, TString);
            iface.sendMessage(bytes);

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
}
