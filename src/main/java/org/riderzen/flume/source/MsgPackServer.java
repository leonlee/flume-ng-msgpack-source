package org.riderzen.flume.source;


import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;
import org.msgpack.util.json.JSON;
import org.msgpack.util.json.JSONUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Map;

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

    public MsgPackServer(MsgPackSource source) {
        this.source = source;
        this.msgPack = new MessagePack();
    }


    @Override
    public int sendMessage(byte[] headersBytes, byte[] bodyBytes) {
        try {
            Event event;
            JSONObject headers = null;
            if (headersBytes != null && bodyBytes.length > 0){
                String headersString =  msgPack.read(headersBytes, Templates.TString);
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
                return NORMAL;
            }

            //noinspection unchecked
            event = EventBuilder.withBody(body, Charset.forName(CHARSET_NAME), headers);

            //@TODO: add supporting batch model in future
            source.getChannelProcessor().processEvent(event);
        } catch (Exception e) {
            logger.error("can't process message", e);
            return ERROR;
        }

        return NORMAL;
    }
}
