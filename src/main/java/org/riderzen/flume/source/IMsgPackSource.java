package org.riderzen.flume.source;

/**
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 下午5:11
 */
public interface IMsgPackSource {
    public int sendMessage(byte[] binary);
}
