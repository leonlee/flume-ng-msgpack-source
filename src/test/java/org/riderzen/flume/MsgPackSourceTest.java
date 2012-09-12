package org.riderzen.flume;

import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Before;
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
    public static final int PORT_NUM = 41414;
    private MsgPackSource source;
    private Channel channel;

    @Before
    public void setUp() {
        source = new MsgPackSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector selector = new ReplicatingChannelSelector();
        selector.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(selector));
    }

    public void startSource() throws InterruptedException {
        Context ctx = new Context();

        ctx.put(MsgPackSource.PORT, String.valueOf(PORT_NUM));
        ctx.put(MsgPackSource.BIND, "0.0.0.0");

        Configurables.configure(source, ctx);

        source.start();
        Thread.sleep(2000l);
    }

    @Test
    public void sourceTest() throws InterruptedException {
        startSource();
        EventLoop cloop = EventLoop.defaultEventLoop();
        Client cli = null;
        MessagePack msgPack = new MessagePack();
        try {
            cli = new Client("localhost", PORT_NUM, cloop);

            IMsgPackSource iface = cli.proxy(IMsgPackSource.class);
            String msg = "hello flume";
            byte[] bytes = msgPack.write(msg, TString);
            iface.sendMessage(bytes);

            Transaction transaction = channel.getTransaction();
            transaction.begin();

            Event event = null;
            while ((event = channel.take()) != null) {
                byte[] binary = event.getBody();
                String result = msgPack.read(binary, TString);

                Assert.assertEquals(msg, result);
                logger.info("taken msg: {}", result);
            }

            transaction.commit();
            transaction.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            source.stop();
        }
    }
}
