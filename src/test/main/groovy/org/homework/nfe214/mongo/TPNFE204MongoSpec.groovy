package org.homework.nfe214.mongo

import com.mongodb.client.DistinctIterable

import static com.mongodb.client.model.Aggregates.*
import static com.mongodb.client.model.Filters.*
import static com.mongodb.client.model.Sorts.*
import static com.mongodb.client.model.Projections.*
import static com.mongodb.client.model.Accumulators.*
import com.mongodb.MongoClient
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import spock.lang.Shared
import spock.lang.Specification

class TPNFE204MongoSpec extends Specification {

    @Shared
    MongoClient mongoClient

    @Shared
    MongoCollection<Document> publisCollection

    def setupSpec() {
        mongoClient = new MongoClient("localhost", 27017)
        MongoDatabase database = mongoClient.getDatabase("DBLP")
        publisCollection = database.getCollection("publis")

    }

    def cleanupSpec() {
        mongoClient.close()
    }

    def "insert data"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("DBLP")
        MongoCollection<Document> collection = database.getCollection("publis")

        Document doc = new Document("type", "Book")
                .append("title", "Modern Database Systems: The Object Model, Interoperability, and Beyond.")
                .append("year", 1995)
                .append("authors", ["Won Kim"])
                .append("source", "DBLP")

        when:
        collection.insertOne(doc)

        then:
        assert true
    }

    // Mongo Shell : db.publis.count({"type":"Book"})
    def "lister tous les livres de typy 'Book'"() {
        when:
        FindIterable<Document> resultSet = publisCollection.find(new Document("type", "Book"))

        then:
        resultSet.size() == 11075
    }
    // https://docs.mongodb.com/manual/tutorial/query-documents/
    // Mongo Shell : db.publis.find({year : {$gte : 2011}});
    def "Liste des publications depuis 2011"() {
        when:
        FindIterable<Document> resultSet = publisCollection.find(gte("year", 2011))

        then:
        resultSet.size() == 29499
    }

    // db.publis.find({"type" : "Book", year : {$gte : 2014}});
    def "Liste des livres depuis 2014"() {
        when:
        FindIterable<Document> resultSet = publisCollection.find(and(gte("year", 2014), eq("type", "Book")))

        then:
        resultSet.size() == 288
    }

    def "Liste des publications de l’auteur « Toru Ishida »"() {
        when:
        FindIterable<Document> resultSet = publisCollection.find(eq("authors", "Toru Ishida"))

        then:
        resultSet.size() == 22
        resultSet.each { println it }
    }

    def "Distinct sur les types de document"() {
        when:
        DistinctIterable<Document> resultSet = publisCollection.distinct("type", String.class)

        then:
        resultSet.size() == 3
    }

    // db.publis.distinct("publisher").length
    def "Liste de tous les éditeurs (type « publisher »), distincts"() {
        when:
        DistinctIterable<Document> resultSet = publisCollection.distinct("publisher", String.class)

        then:
        resultSet.size() == 539
    }
    // db.publis.distinct("authors").length;
    def "Liste de tous les auteurs  distincts"() {
        when:
        DistinctIterable<Document> resultSet = publisCollection.distinct("authors", String.class)

        then:
        resultSet.size() == 158772
    }

    // https://docs.mongodb.com/manual/core/aggregation-pipeline/
    // db.publis.aggregate([{$match:{authors : "Toru Ishida"}}, { $sort : { booktitle : 1, "pages.start" : 1 } }]);
    def "Trier les publications de « Toru Ishida » par titre de livre et par page de début ;"() {
        when:
        def resultSet = publisCollection.aggregate([match(eq("authors", "Toru Ishida")), sort(ascending("booktitle", "pages.start"))])

        then:
        resultSet.each {
            println it
        }
    }

    // https://docs.mongodb.com/manual/core/aggregation-pipeline/
    // db.publis.aggregate([{$match:{authors : "Toru Ishida"}}, {$sort : { booktitle : 1, "pages.start" : 1 }}, {$project : {title : 1, pages : 1}}]);
    def "Trier les publications de « Toru Ishida » par titre de livre et par page de début ; projection uniquement du titre et des pages"() {
        when:
        def resultSet = publisCollection.aggregate([match(eq("authors", "Toru Ishida")), sort(ascending("booktitle", "pages.start")), project(include("title", "pages"))])

        then:
        resultSet.each {
            println it
        }
    }

    // db.publis.aggregate([{$match:{authors : "Toru Ishida"}}, {$group:{_id:null, total : { $sum : 1}}}]);
    // db.publis.aggregate([{$match:{authors : "Toru Ishida"}}, {$count:"title"}]);
    def "Compter le nombre de ses publications"() {
        when:
        def resultSet = publisCollection.aggregate([match(eq("authors", "Toru Ishida")), count("title")])

        then:
        resultSet.each {
            println it
        }
    }

    def "Compter le nombre de publications depuis 2011 et par type ;"() {
        when:
        def resultSet
        resultSet = publisCollection.aggregate([match(gte("year", 2011)), group('$type', sum("total", 1))])

        then:
        resultSet.each {
            println it
        }
    }

    def "Compter le nombre de publications par auteur et trier le résultat par ordre croissant"() {
        when:
        def resultSet
        resultSet = publisCollection.aggregate([ unwind('$authors'), group('$authors', sum("total", 1)), sort(ascending('total'))])

        then:
        resultSet.each {
            println it
        }
    }


}
