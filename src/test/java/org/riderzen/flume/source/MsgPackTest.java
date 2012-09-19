package org.riderzen.flume.source;

import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.rpc.Client;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.tList;
import static org.testng.Assert.*;


public class MsgPackTest {
    @Message
    public static class TestMsg {
        public String name;
        public long version;
    }

    @Test(groups = {"unit"})
    public void msgTest() throws Exception {
        TestMsg src = new TestMsg();
        src.name = "msgpack";
        src.version = 1;

        MessagePack msgpack = new MessagePack();
        byte[] bytes = msgpack.write(src);

        TestMsg dist = msgpack.read(bytes, TestMsg.class);

        assertEquals(src.name, dist.name, "the name of message should be the same");
        assertEquals(src.version, dist.version, "the version of message should be the same");
    }

    @Test(groups = {"unit"})
    public void dynamicTest() throws Exception {
        List<String> src = new ArrayList<String>();
        src.add("msgpack");
        src.add("kumofs");
        src.add("viver");

        MessagePack msgpack = new MessagePack();
        byte[] raw = msgpack.write(src);

        List<String> dist1 = msgpack.read(raw, tList(TString));
        assertNotNull(dist1);
        assertEquals(src.size(), dist1.size());
        for (int i = 0; i < src.size(); i++) {
            assertEquals(src.get(i), dist1.get(i));
        }

        Value dynamic = msgpack.read(raw);
        List<String> dist2 = new Converter(dynamic).read(tList(TString));
        assertNotNull(dist2);
        assertEquals(src.size(), dist2.size());
        for (int i = 0; i < src.size(); i++) {
            assertEquals(src.get(i), dist2.get(i));
        }

    }
//    @Test
    public void rpcServerTest() {
        final EventLoop loop = EventLoop.defaultEventLoop();

        Server srv = new Server();
        srv.serve(new ServerApp(loop,srv));
        try {
            srv.listen(1985);
//            loop.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test(groups = {"unit"})
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

            assertEquals("hello", res);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static interface RPCInterface {
        String helloRpc(String msg, int a);
        void stop();
        int testMap(Map<String, String> map);
    }
}

