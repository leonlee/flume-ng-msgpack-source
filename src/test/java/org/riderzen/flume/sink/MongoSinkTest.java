package org.riderzen.flume.sink;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.apache.commons.collections.MapUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * User: guoqiang.li
 * Date: 12-9-12
 * Time: 下午3:31
 */
public class MongoSinkTest {
    private static Mongo mongo;
    public static final String DBNAME = "myDb";

    @BeforeClass(groups = {"dev"})
    public static void setup() throws UnknownHostException {
        mongo = new Mongo("localhost", 27017);
    }

    @AfterClass(groups = {"dev"})
    public static void tearDown() {
        mongo.dropDatabase(DBNAME);
    }

    @Test(groups = "dev")
    public void dbTest() {
        DB db = mongo.getDB(DBNAME);
        db.getCollectionNames();
        List<String> names = mongo.getDatabaseNames();

        assertNotNull(names);
        boolean hit = false;

        for (String name : names) {
            if (DBNAME.equals(name)) {
                hit = true;
                break;
            }
        }

        assertTrue(hit);
    }

    @Test(groups = "dev")
    public void collectionTest() {
        DB db = mongo.getDB(DBNAME);
        DBCollection myCollection = db.getCollection("myCollection");
        myCollection.save(new BasicDBObject(MapUtils.putAll(new HashMap(), new Object[]{"name", "leon", "age", 33})));
        myCollection.findOne();

        Set<String> names = db.getCollectionNames();

        assertNotNull(names);
        boolean hit = false;

        for (String name : names) {
            if ("myCollection".equals(name)) {
                hit = true;
                break;
            }
        }

        assertTrue(hit);
    }


}
