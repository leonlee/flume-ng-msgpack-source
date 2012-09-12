package org.riderzen.flume;

import org.msgpack.rpc.Server;
import org.msgpack.rpc.loop.EventLoop;

/**
 *
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 上午10:35
 */
public class ServerApp {
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

    public int hello1(int a) {
        return a + 1;
    }

    public int hello2(int a) {
        return a + 2;
    }
}
