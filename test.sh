#!/bin/bash

mvn exec:java \
	-Dexec.mainClass="com.pushtechnology.example.mongo.Adapter" \
	-Dexec.args="-du ws://welcomingspeedyAtlas.eu.reappt.io:80 -dp mongoAdapter -dc mongoMongo -mh localhost:20000 -md truefx -mc currencies"
