package org.riderzen.flume.sink;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: guoqiang.li
 * Date: 12-9-12
 * Time: 下午3:31
 */
public class MongoSink extends AbstractSink implements Configurable {
    private static Logger logger = LoggerFactory.getLogger(MongoSink.class);

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String MODEL = "model";
    public static final String DB_NAME = "db";
    public static final String COLLECTION = "collection";
    public static final String NAME_PREFIX = "MongSink_";
    public static final String BATCH_SIZE = "batch";

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 27017;
    public static final String DEFAULT_DB = "events";
    public static final String DEFAULT_COLLECTION = "events";
    public static final int DEFAULT_BATCH = 100;

    private static AtomicInteger counter = new AtomicInteger();

    private Mongo mongo;
    private DB db;

    private String host;
    private int port;
    private String username;
    private String password;
    private CollectionModel model;
    private String dbName;
    private String collectionName;
    private int batchSize;

    @Override
    public void configure(Context context) {
        setName(NAME_PREFIX + counter.getAndIncrement());

        host = context.getString(HOST, DEFAULT_HOST);
        port = context.getInteger(PORT, DEFAULT_PORT);
        username = context.getString(USERNAME);
        password = context.getString(PASSWORD);
        model = CollectionModel.valueOf(context.getString(MODEL, CollectionModel.single.name()));
        dbName = context.getString(DB_NAME, DEFAULT_DB);
        collectionName = context.getString(COLLECTION, DEFAULT_COLLECTION);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH);

        logger.info("MongoSink {} context { host:{}, port:{}, username:{}, password:{}, model:{}, dbName:{}, collectionName:{}, batch: {} }",
                new Object[]{getName(), host, port, username, password, model, dbName, collectionName, batchSize});
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", getName());

        try {
            mongo = new Mongo(host, port);
            db = mongo.getDB(dbName);
        } catch (UnknownHostException e) {
            logger.error("Can't connect to mongoDB", e);
        }

        super.start();
        logger.info("Started {}.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("{} start to process event", getName());

        Status status = Status.READY;
        try {
            status = parseEvents();
        } catch (Exception e) {
            logger.error("can't process events", e);
        }
        logger.debug("{} processed event", getName());
        return status;
    }

    private void saveEvents(Map<String, List<DBObject>> eventMap) {
        if (eventMap.isEmpty()) {
            logger.debug("eventMap is empty");
            return;
        }

        for (String collectionName : eventMap.keySet()) {
            List<DBObject> docs = eventMap.get(collectionName);
            if (logger.isDebugEnabled()) {
                logger.debug("collection: {}, length: {}", collectionName, docs.size());
            }
            //Warning: please change the WriteConcern level if you need high datum consistence.
            CommandResult result = db.getCollection(collectionName).insert(docs, WriteConcern.NORMAL).getLastError();
            if (result.ok()) {
                String errorMessage = result.getErrorMessage();
                if (errorMessage != null) {
                    logger.error("can't insert documents with error: {} ", errorMessage);
                    logger.error("with exception", result.getException());

                    throw new MongoException(errorMessage);
                }
            } else {
                logger.error("can't get last error");
            }
        }
    }

    private Status parseEvents() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = null;
        Map<String, List<DBObject>> eventMap = new HashMap<String, List<DBObject>>();
        try {
            tx = channel.getTransaction();
            tx.begin();

            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                } else {
                    switch (model) {
                        case single:
                            if (!eventMap.containsKey(collectionName)) {
                                eventMap.put(collectionName, new ArrayList<DBObject>());
                            }

                            List<DBObject> docs = eventMap.get(collectionName);
                            addEventToList(docs, event);

                            break;
                        case dynamic:
                            Map<String, String> headers = event.getHeaders();
                            String dynamicCollection = headers.get(COLLECTION);

                            if (!eventMap.containsKey(dynamicCollection)) {
                                eventMap.put(dynamicCollection, new ArrayList<DBObject>());
                            }

                            List<DBObject> documents = eventMap.get(dynamicCollection);
                            addEventToList(documents, event);

                            break;
                        default:
                            logger.error("can't support model: {}, please check configuration.", model);
                    }
                }
            }
            if (!eventMap.isEmpty()) {
                saveEvents(eventMap);
            }

            tx.commit();
        } catch (Exception e) {
            logger.error("can't process events", e);
            if (tx != null) {
                tx.rollback();
            }

            throw new EventDeliveryException(e);
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
        return status;
    }

    private List<DBObject> addEventToList(List<DBObject> documents, Event event) {
        if (documents == null) {
            documents = new ArrayList<DBObject>(batchSize);
        }

        DBObject eventJson = (DBObject) JSON.parse(new String(event.getBody()));
        documents.add(eventJson);

        return documents;
    }

    public static enum CollectionModel {
        dynamic, single
    }
}
