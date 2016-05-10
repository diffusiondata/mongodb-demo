package com.pushtechnology.example.mongo;

import com.beust.jcommander.Parameter;

public class CommandlineArgs {

    @Parameter(names={"--mongoHost", "-mh"}, required=true, description="host server to connect to in format host[:port]")
    private String mongoHost;
    
    @Parameter(names={"--mongoDatabase", "-md"}, required=true, description="name of a MongoDB database")
    private String mongoDatabase;
    
    @Parameter(names={"--mongoCollection", "-mc"}, required=true, description="name of a MongoDB collection")
    private String mongoCollection;
    
    
    @Parameter(names={"--diffusionURL", "-du"}, required=true)
    private String diffusionURL;
    
    @Parameter(names={"--diffusionPrincipal", "-dp"}, required=false)
    private String diffusionPrincipal;
    
    @Parameter(names={"--diffusionCredentials", "-dc"}, required=false)
    private String diffusionCredentials;
    
    @Parameter(names={"--topic", "-dt"}, required=false, description="Root adapter topic")
    private String topic = "MongoDB";

    /**
     * @return the mongoHost
     */
    String getMongoHost() {
        return mongoHost;
    }

    /**
     * @return the mongoCollection
     */
    String getMongoCollection() {
        return mongoCollection;
    }

    /**
     * @return the diffusionURL
     */
    String getDiffusionURL() {
        return diffusionURL;
    }

    /**
     * @return the diffusionPrincipal
     */
    String getDiffusionPrincipal() {
        return diffusionPrincipal;
    }

    /**
     * @return the diffusionCredentials
     */
    String getDiffusionCredentials() {
        return diffusionCredentials;
    }

    /**
     * Returns mongoDatabase.
     *
     * @return the mongoDatabase
     */
    String getMongoDatabase() {
        return mongoDatabase;
    }

    /**
     * Returns topic.
     *
     * @return the topic
     */
    String getTopic() {
        return topic;
    }
}
