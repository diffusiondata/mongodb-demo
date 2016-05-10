package com.pushtechnology.example.mongo;

import static com.mongodb.CursorType.TailableAwait;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.*;

import java.util.concurrent.TimeUnit;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class OploggingTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(OploggingTest.class);
    
    @Test
    public void oplogTest() throws Exception {
        final MongoClient mongoClient = new MongoClient("localhost:20001");
        final MongoCollection<Document> collection = mongoClient
            .getDatabase("local")
            .getCollection("oplog.rs");

        final BsonTimestamp now = new BsonTimestamp((int)(System.currentTimeMillis()/1000), 1);        
        System.err.printf("now=%s\n", now);
        final Bson filter = 
            and( 
                or(
                    in("op", "i", "u","d"),
                    and(
                        eq("op", "c"), // command
                        exists("o.drop") // drop collection
                    )
                )
                ,gt("ts", now)
            );

        while(true) {            
            try (final MongoCursor<Document> cursor = collection.find(filter)
                .cursorType(TailableAwait)
                .maxAwaitTime(1, TimeUnit.SECONDS)
                .noCursorTimeout(true)
                .iterator()) {

                int i = 0; 
                while (cursor.hasNext()) {
                    final Document update = cursor.next();
                    LOG.info("received[{}]: {}", i++, update.toJson());                
                }
            }
        }

    }

}
