package org.homework.nfe214.mongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

/**
 * Container docker :
 * docker run --name remy-mongo -p 27017:27017 -v /home/developper/dev/nfe214-mongo/src/main/resources:/var/local -d mongo
 * docker exec -t -i remy-mongo /bin/bash
 * cd /var/local
 * mongoimport -d nfe204 -c movies --file movies.json --jsonArray
 *
 *
 * reference : http://mongodb.github.io/mongo-java-driver/3.4/driver/getting-started/quick-start/
 */
class MongoMain {

    static void main(String[] args) {
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 )
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> collection = database.getCollection("movies")
        println collection.count()
    }
}
