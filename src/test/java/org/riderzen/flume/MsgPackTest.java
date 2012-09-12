package org.riderzen.flume;

import org.junit.Assert;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;

import java.util.ArrayList;
import java.util.List;

import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.tList;


public class MsgPackTest {
    @Message
    public static class TestMsg {
        public String name;
        public long version;
    }

    @Test
    public void msgTest() throws Exception {
        TestMsg src = new TestMsg();
        src.name = "msgpack";
        src.version = 1;

        MessagePack msgpack = new MessagePack();
        byte[] bytes = msgpack.write(src);

        TestMsg dist = msgpack.read(bytes, TestMsg.class);

        Assert.assertEquals("the name of message should be the same", src.name, dist.name);
        Assert.assertEquals("the version of message should be the same", src.version, dist.version);
    }

    @Test
    public void dynamicTest() throws Exception {
        List<String> src = new ArrayList<String>();
        src.add("msgpack");
        src.add("kumofs");
        src.add("viver");

        MessagePack msgpack = new MessagePack();
        byte[] raw = msgpack.write(src);

        List<String> dist1 = msgpack.read(raw, tList(TString));
        Assert.assertNotNull(dist1);
        Assert.assertEquals(src.size(), dist1.size());
        for (int i = 0; i < src.size(); i++) {
            Assert.assertEquals(src.get(i), dist1.get(i));
        }

        Value dynamic = msgpack.read(raw);
        List<String> dist2 = new Converter(dynamic).read(tList(TString));
        Assert.assertNotNull(dist2);
        Assert.assertEquals(src.size(), dist2.size());
        for (int i = 0; i < src.size(); i++) {
            Assert.assertEquals(src.get(i), dist2.get(i));
        }

    }
//    @Test
    public void rpcServerTest() {
        final EventLoop loop = EventLoop.defaultEventLoop();

        Server srv = new Server();
        srv.serve(new ServerApp(loop,srv));
        try {
            srv.listen(1985);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void rpcClientTest() {
        new Thread() {
            @Override
            public void run() {
                rpcServerTest();
            }
        }.start();

        EventLoop cloop = EventLoop.defaultEventLoop();
        Client cli = null;
        try {
            cli = new Client("localhost", 1985, cloop);

            RPCInterface iface = cli.proxy(RPCInterface.class);

            String res = iface.helloRpc("hello", 1);

            System.out.println("res: " + res);

            Assert.assertEquals("hello", res);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static interface RPCInterface {
        String helloRpc(String msg, int a);
        void stop();
    }
}

