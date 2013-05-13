package org.riderzen.flume.source;


import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 下午4:42
 */
public class MsgPackServer implements IMsgPackSource {
    private static final Logger logger = LoggerFactory.getLogger(MsgPackServer.class);
    public static final int NORMAL = 0;
    public static final int ERROR = 1;
    public static final String BODY = "body";
    public static final String CHARSET_NAME = "UTF-8";

    private MsgPackSource source;
    private MessagePack msgPack;
    private ThreadPoolExecutor pool;
    private int queueSize;

    public MsgPackServer(MsgPackSource source) {
        this.source = source;
        this.msgPack = new MessagePack();
        this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1000);
        this.queueSize = source.getQueueSize();
    }


    @Override
    public int sendMessage(byte[] headersBytes, byte[] bodyBytes) {
        try {
            int queueSize = this.pool.getQueue().size();
            if (queueSize > queueSize) {
                logger.error("message was overflowed, the queue size is {}, the max size is {}", queueSize, queueSize);
                if (bodyBytes != null) {
                    logger.error("dropped msg: {}", msgPack.read(bodyBytes, Templates.TString));
                }
                return ERROR;
            }
            processMessage(headersBytes, bodyBytes);

        } catch (Exception e) {
            logger.error("can't process message", e);
            return ERROR;
        }
        return NORMAL;
    }

    private void processMessage(final byte[] headersBytes, final byte[] bodyBytes) {
        this.pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Event event;
                    JSONObject headers = null;
                    if (headersBytes != null && bodyBytes.length > 0) {
                        String headersString = msgPack.read(headersBytes, Templates.TString);
                        if (logger.isDebugEnabled()) {
                            logger.debug("received message headers:{}", headersString);
                        }
                        JSONParser parser = new JSONParser();
                        headers = (JSONObject) parser.parse(headersString);
                    }
                    String body = null;
                    if (bodyBytes != null) {
                        body = msgPack.read(bodyBytes, Templates.TString);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("received message body: {}", body);
                    }
                    if (body == null) {
                        return;
                    }
                    //noinspection unchecked
                    event = EventBuilder.withBody(body, Charset.forName(CHARSET_NAME), headers);
                    //@TODO: add supporting batch model in future
                    source.getChannelProcessor().processEvent(event);
                } catch (Throwable e) {
                    logger.error("can't process message", e);
                }
            }
        });
    }

}
