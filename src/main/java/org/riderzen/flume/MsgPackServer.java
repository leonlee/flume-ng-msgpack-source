package org.riderzen.flume;


import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: guoqiang.li
 * Date: 12-9-11
 * Time: 下午4:42
 */
public class MsgPackServer implements IMsgPackSource {
    private static final Logger logger = LoggerFactory.getLogger(MsgPackServer.class);
    public static final int NORMAL = 0;
    public static final int ERROR = 1;
    private MsgPackSource source;
    private MessagePack msgPack;

    public MsgPackServer(MsgPackSource source) {
        this.source = source;
        this.msgPack = new MessagePack();
    }


    @Override
    public int sendMessage(byte[] binary) {
        try {
            Event event;
            if (source.isNeedDecode()) {
                String message = msgPack.read(binary, Templates.TString);
                if (logger.isDebugEnabled()) {
                    logger.debug("received message: {}", message);
                }
                event = EventBuilder.withBody(message.getBytes());
            } else {
                event = EventBuilder.withBody(binary);
            }

            source.getChannelProcessor().processEvent(event);
        } catch (Exception e) {
            logger.error("can't process message", e);
            return ERROR;
        }

        return NORMAL;
    }
}
