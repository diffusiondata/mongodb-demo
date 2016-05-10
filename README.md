# MongoDB Diffusion adapter demo

This project contains example code that demonstrates reflection of a MongoDB collection
into part of the Diffusion/Reappt topic tree.

MongoDB replication must be enabled, and the adapter should be connected to the
primary instance.

## Yes, but why?

Reflecting Mongo documents as topics makes it possible to subscribe to changes effecting that document.
 
## TL;DR

Reflect a MongoDB collection into the Diffusion/Reappt topic tree.

## Simplest establishment of a suitable MongoDB server

Establish the simplest MongoDB with replication enabled.

    % mongodb —nodb
    > replicaSet = new ReplSetTest({"nodes" : 1, "nodeOptions": {"dbpath": "/tmp/mongodb"} })
    > replicaSet.startSet()
    > replicaSet.initiate()

This may make that terminal quite noisy, so replace it with another. Log into 
your Mongo server:

    % mongo localhost:20001
    > use someMongoDatabase
    > for (i=0; i<100; i++) { db.someMongoCollection.insert({count: i}) }
    
Which will create database `someMongoDatabase` and collection `someMongoCollection` 
holding 100 brief documents.
        

## Usage

Start your Diffusion server, then start the adapter:

    java com.pushtechnology.example.mongo.Adapter \
        -du ws://localhost:8080 -dp admin -dc password \
        -mh localhost:20001 -md someMongoDatabase -mc someMongoCollection

Documents from `someMongoDatabase.someMongoCollection` will appear as topics under
`MongoDB/someMongoDatabase/someMongoCollection` where the OjectID is reused as 
the topic name.

  