package org.homework.nfe214.mongo

import com.mongodb.client.DistinctIterable
import com.mongodb.client.MapReduceIterable

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
        resultSet = publisCollection.aggregate([unwind('$authors'), group('$authors', sum("total", 1)), sort(ascending('total'))])

        then:
        resultSet.each {
            println it
        }
    }

    def "Pour chaque document de type livre, émettre le document avec pour clé title"() {
        setup:
        String map = """
            function() {
                if(this.type == 'Book') {
                    emit(this.title, this);
                }
            }
        """
        String reduce = """
            function(title, docs) {
               var result = new Object();
               result.title = title;
               result.docs = docs;
               return result;
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "Pour chacun de ses livres, donner le nombre de ses auteurs"() {
        setup:
        String map = """
            function() {
                if(this.type == 'Book') {
                    emit(this.title, this.authors.length);
                }
            }
        """
        String reduce = """
            function(title, nbAuthors) {  
                var result = new Object();
                result.nbAuthors = nbAuthors;             
                return result;
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "Pour chaque document ayant 'booktitle' (chapitre) publié par Springer, donner le nombre de ses chapitres"() {
        setup:
        String map = """
            function() {
              if(this.publisher == 'Springer' && this.booktitle) {
                  emit(this.booktitle, 1);
              }
            }
        """
        String reduce = """
            function(key, values) {  
                return Array.sum(values);
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "Pour chaque éditeur « Springer », donner le nombre de publication par année"() {
        setup:
        String map = """
            function() {
              if(this.publisher == 'Springer') {
                  emit(this.year, 1);
              }
            }
        """
        String reduce = """
            function(key, values) {  
                return Array.sum(values);
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "Pour chaque clé « publisher & année » (pour ceux qui ont un publisher), donner le nombre de publications"() {
        setup:
        String map = """
            function() {
               if(this.publisher) {
                var document = new Object();
                document.publisher = this.publisher
                document.year = this.year              
                emit(document, 1);
               }
            }
        """
        String reduce = """
            function(key, values) {  
                return Array.sum(values);
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "6 - Pour l’auteur « Toru Ishida », donner le nombre de publication par année"() {
        setup:
        String map = """
            function() {
                if(Array.contains(this.authors, "Toru Ishida")) {
                    emit(this.year, 1);
                }
            }
        """
        String reduce = """
            function(key, values) {  
                return Array.sum(values);
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "7 - Pour l’auteur « Toru Ishida », donner le nombre moyen de pages pour ses articles (type Article)"() {
        setup:
        String map = """
            function() {
                if(this.type != 'Article') {
                    return;
                }
                if(!Array.contains(this.authors, "Toru Ishida")) {
                    return;
                }
                emit("Article", this.pages.end - this.pages.start)
            }
        """
        String reduce = """
            function(key, values) {  
                return Array.avg(values);
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    //too much data for in memory map/reduce
    def "8 - Pour chaque auteur donner le titre de ses publications"() {
        setup:
        String map = """
            function() {
                for(var idx = 0; idx < this.authors.length; idx ++) {
                    emit(this.authors[idx], this.title);
                }
            }
        """
        String reduce = """
            function(key, values) {  
                return {"livres":values};
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    //too much data for in memory map/reduce
    def "9 - Pour chaque auteur, donner le nombre de publications associé à chaque année"() {
        setup:
        String map = """
            function() {
                for(var idx = 0; idx < this.authors.length; idx ++) {
                    
                    emit({author:this.authors[idx], year:this.year}, 1);
                }
            }
        """
        String reduce = """
            function(key, values) {  
                return Array.sum(values);
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "10 - Pour chaque auteur, donner le nombre de publications associé à chaque année"() {
        setup:
        String map = """
            function() {
                if(this.publisher != 'Springer') {
                    return;
                }
            
                for(var idx = 0; idx < this.authors.length; idx ++) {
                    emit(this.year, this.authors);
                }
            }
        """
        String reduce = """
            function(year, authorsWithDoublon) {  
                var countedAuthors = []
                for(var idx = 0; idx < authorsWithDoublon.length; idx ++) {
                    if(Array.contains(countedAuthors, authorsWithDoublon[idx])) {
                        continue
                    }
                    countedAuthors.push(authorsWithDoublon[idx]);
                
                }
                return countedAuthors.length;
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "11 - Compter les publications de plus de 3 auteurs"() {
        setup:
        String map = """
            function() {
                if(this.authors.length > 3) {
                    emit("COUNT",1);
                }
                
            }
        """
        String reduce = """
            function(count, values) {
                return Array.sum(values);  
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "12 - Donner pour chaque publieur, donner le nombre moyen de pages par publication"() {
        setup:
        String map = """
            function() {
                if(!this.publisher || !this.pages || !this.pages.end || !this.pages.start) {
                    return;
                }
                emit(this.publisher, this.pages.end - this.pages.start);
            }
        """
        String reduce = """
            function(publisher, pages) {
                return Array.avg(pages);  
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "13 - Pour chaque auteur, donner le minimum et le maximum des années, ainsi que le nombre de publication totale"() {
        setup:
        String map = """
            function() {            
                for(var idx = 0; idx < this.authors.length; idx ++) {
                    emit(this.authors[idx], {year:this.year});
                }
            }
        """
        String reduce = """
            function(author, documents) {
            
                var minYear = 3000;
                var maxYear = 0;
                for(var i = 0; i < documents.length; i++) {
                    minYear = Math.min(minYear, documents[i].year);
                    maxYear = Math.max(maxYear, documents[i].year);
                }  
                return {min:minYear, max:maxYear, number:documents.length};
            }    
        """
        when:
        MapReduceIterable<?> result = publisCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }
}
