package com.pushtechnology.example.mongo;

import static com.mongodb.CursorType.TailableAwait;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.or;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.callbacks.TopicTreeHandler;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.AddCallback;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl.RemoveCallback;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.UpdateSource;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.Updater;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.Updater.UpdateCallback;
import com.pushtechnology.diffusion.client.features.control.topics.TopicUpdateControl.ValueUpdater;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.datatype.json.JSON;

/**
 * An Adapter that reflects the JSON documents within a Mongo collection
 * as topics in a Diffusion/Reappt server.
 *
 * @author mcowie
 */
public final class Adapter {

    private static final Logger LOG = LoggerFactory.getLogger(Adapter.class);

    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> oplog;
    private final TopicControl topicControl;
    private final Map<ObjectId, String> topicPaths = new HashMap<>();
    private final ValueUpdater<JSON> valueUpdater;
    private final String topicRoot;

    private final String commandNamespaceName;
    private final String collectionNamespaceName;

    /**
     * Command line bootstrap method.
     */
    public static void main(String[] argv) throws Exception {

        final CommandlineArgs args = new CommandlineArgs();
        final JCommander jc = new JCommander(args);
        try {
            jc.parse(argv);
        }
        catch (ParameterException ex) {
            jc.usage();
            System.exit(1);
        }

        final MongoClient mongoClient = new MongoClient(args.getMongoHost());
        final MongoCollection<Document> collection = getCollection(mongoClient,
            args.getMongoDatabase(),
            args.getMongoCollection());

        final MongoCollection<Document> oplog = getCollection(mongoClient,
            "local",
            "oplog.rs");

        final Session session = Diffusion.sessions()
            .principal("admin")
            .password("password")
            .noReconnection()
            .open("ws://localhost:8080");

        final String topicRoot = args.getTopic() + "/" +
            args.getMongoDatabase() + "/" +
            args.getMongoCollection();

        Adapter.build(session, collection, oplog, topicRoot).run();
        mongoClient.close();
    }

    private static MongoCollection<Document> getCollection(
        final MongoClient mongoClient, String databaseName,
        String collectionName) {
        final MongoDatabase database = mongoClient.getDatabase(databaseName);
        return database.getCollection(collectionName);
    }

    /**
     * Build an Adapter.
     * <P>
     *
     * @param session Configured Diffusion session.
     * @param collection Source of documents for reflection.
     * @param oplog Collection holding the MongoDB replication operation log.
     * @param topicRoot Topic under which other topics are to be created.
     * @return a ready-to-use Adapter
     * @throws InterruptedException if the current thread was interrupted while waiting for update rights
     * @throws TimeoutException if the adapter takes more than 10s to exclusive update rights on the topic tree
     */
    public static Adapter build(Session session,
        MongoCollection<Document> collection,
        MongoCollection<Document> oplog,
        String topicRoot) throws InterruptedException, TimeoutException {
        final Exchanger<ValueUpdater<JSON>> exchanger = new Exchanger<>();

        final TopicUpdateControl topicUpdateControl =
            session.feature(TopicUpdateControl.class);
        topicUpdateControl.registerUpdateSource(topicRoot,
            new UpdateSource.Default() {
                @Override
                public void onActive(String topicPath, Updater updater) {
                    try {
                        exchanger.exchange(updater.valueUpdater(JSON.class));
                    }
                    catch (InterruptedException ex) {
                        throw new AssertionError(ex);
                    }
                }
            });

        final ValueUpdater<JSON> valueUpdater =
            exchanger.exchange(null, 10, TimeUnit.SECONDS);
        final TopicControl topicControl = session.feature(TopicControl.class);

        topicControl.removeTopicsWithSession(topicRoot,
            new TopicTreeHandler.Default());

        final MongoNamespace namespace =
            new MongoNamespace(collection.getNamespace().getDatabaseName(),
                "$cmd");

        return new Adapter(topicControl,
            valueUpdater,
            collection,
            oplog,
            namespace.toString(),
            topicRoot);
    }

    private Adapter(TopicControl topicControl,
        ValueUpdater<JSON> valueUpdater,
        MongoCollection<Document> collection,
        MongoCollection<Document> oplog,
        String commandNamespaceName,
        String topicRoot) {

        this.topicControl = topicControl;
        this.valueUpdater = valueUpdater;
        this.collection = collection;
        this.oplog = oplog;
        this.topicRoot = topicRoot;

        this.commandNamespaceName =
            new MongoNamespace(collection.getNamespace().getDatabaseName(),
                "$cmd").toString();
        this.collectionNamespaceName = collection.getNamespace().toString();
    }

    /**
     * Enumerate all documents in the collection, then relay changes.
     */
    private void run() {
        final long timeNow = System.currentTimeMillis();
        long topicCount = 0;
        LOG.info("Trascribing topics from {} to {}", collection.getNamespace(),
            topicRoot);
        try (
            final MongoCursor<Document> cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                final Document update = cursor.next();
                LOG.debug("received: {}", update.toJson());

                transcribeDocument(update);
                topicCount++;
            }
        }
        LOG.info("Transcribed {} topics", topicCount);
        relayChanges(timeNow);
    }

    /**
     * Create new topic and set its state.
     */
    private void transcribeDocument(Document update) {
        final ObjectId id = update.getObjectId("_id");
        final String topicPath = topicPaths.get(id);

        if (topicPath == null) {
            final String newTopicPath = topicRoot + "/" + id.toString();
            LOG.info("Creating {}", newTopicPath);
            topicPaths.put(id, newTopicPath);
            topicControl.addTopic(newTopicPath,
                TopicType.JSON,
                toBytes(update.toJson()),
                new AddCallback.Default());
        }
        else {
            LOG.info("Updating {}", topicPath);
            valueUpdater.update(topicPath, toBytes(update.toJson()),
                new UpdateCallback.Default());
        }
    }

    /**
     * Subscribe to {@code local.oplog.rs}, listening for relevant changes.
     */
    private void relayChanges(long timeNow) {
        LOG.info("Relaying changes {} to {}", oplog.getNamespace(), topicRoot);

        final BsonTimestamp now = new BsonTimestamp((int) (timeNow / 1000), 1);
        final Bson filter = and(
            gt("ts", now),
            or(
                in("op", "i", "u", "d"), // insert, update & delete document
                and(eq("op", "c"), exists("o.drop")) // drop collection
            ));

        try (final MongoCursor<Document> cursor = oplog.find(filter)
            .cursorType(TailableAwait)
            .maxAwaitTime(1, TimeUnit.SECONDS)
            .noCursorTimeout(true)
            .iterator()) {

            while (cursor.hasNext()) {
                final Document document = cursor.next();
                final String op = document.get("op", String.class);
                switch (op) {
                case "i": // Insert
                    processInsert(document);
                    break;
                case "u": // Update
                    processUpdate(document);
                    break;
                case "d": // Delete
                    processDelete(document);
                    break;
                case "c": // Command
                    processCommand(document);
                    break;
                default:
                    LOG.warn("Unexpected 'op' value in event: {}", op);
                }
            }
        }
    }

    private void processInsert(Document document) {
        if (matchesCollection(document)) {
            transcribeDocument(document.get("o", Document.class));
        }
    }

    private void processDelete(Document document) {
        if (matchesCollection(document)) {
            final ObjectId id =
                document.get("o", Document.class).getObjectId("_id");
            final String topicPath = topicPaths.remove(id);

            topicControl.removeTopics(">" + topicPath,
                new RemoveCallback.Default());
        }
    }

    private void processUpdate(Document document) {
        if (matchesCollection(document)) {
            final ObjectId id =
                document.get("o2", Document.class).getObjectId("_id");
            try (final MongoCursor<Document> cursor =
                collection.find(eq("_id", id)).iterator()) {
                if (!cursor.hasNext()) {
                    LOG.error("Cannot find document with ID {}", id);
                }
                transcribeDocument(cursor.next());
            }
        }
    }

    private void processCommand(Document update) {
        // db.collection.drop()
        if (commandNamespaceName.equals(update.get("ns")) &&
            update.containsKey("o") &&
            update.get("o", Document.class).containsKey("drop") &&
            collection.getNamespace().getCollectionName()
                .equals(update.get("o", Document.class).get("drop"))) {

            // Remove the descendants of topicRoot .
            topicControl.removeTopics("?" + topicRoot + "/",
                new RemoveCallback.Default());
            topicPaths.clear();
        }
    }

    private boolean matchesCollection(Document document) {
        return collectionNamespaceName.equals(document.getString("ns"));
    }

    private static JSON toBytes(String json) {
        return Diffusion.dataTypes().json().fromJsonString(json);
    }

}
