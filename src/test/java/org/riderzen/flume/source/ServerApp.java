package org.riderzen.flume.source;

import org.msgpack.MessagePack;
import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;
import org.msgpack.template.Template;
import org.msgpack.template.Templates;
import org.msgpack.type.MapValue;
import org.msgpack.util.json.JSON;

import java.io.IOException;
import java.util.Map;


/**
 *
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 上午10:35
 */
public class ServerApp implements MsgPackTest.RPCInterface{
    private EventLoop loop;
    private Server srv;
    public ServerApp(EventLoop loop, Server srv) {
        this.loop = loop;
        this.srv = srv;
    }

    public String helloRpc(String msg, int a) {
        System.out.println("msg: " + msg + " a:" + a);

        return msg;
    }

    public void stop() {
        System.out.println("stopping...");
        srv.close();
        loop.shutdown();
    }

    @Override
    public int testMap(Map<String, String> map) {
        return 0;
    }

    public int hello1(int a) {
        return a + 1;
    }

    public int hello2(int a) {
        return a + 2;
    }

    public int testMap(byte[] args) throws IOException {
//        for (String arg : args) {
            System.out.println("arg = " + args);
        MessagePack mp = new MessagePack();
        System.out.println("mp.read(args, Templates.TString) = " + mp.read(args, Templates.tMap(Templates.TString, Templates.TString)));
//        }
        return 0;
    }
}
