package org.homework.nfe214.mongo

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

/**
 * Resource : http://webscope.bdpedia.fr/
 * Container docker :
 * docker run --name remy-mongo -p 27017:27017 -v /home/developper/dev/nfe214-mongo/src/main/resources:/var/local -d mongo
 * docker exec -t -i remy-mongo /bin/bash
 * cd /var/local
 * mongoimport -d nfe204 -c movies --file movies.json --jsonArray
 * Dans le container :
 * mongo
 * > use nfe204
 * > db.movies.find().pretty()
 *
 * reference : http://mongodb.github.io/mongo-java-driver/3.4/driver/getting-started/quick-start/
 *
 * Pour jointure map/reduce TP *
 * mongoimport -d moviesref -c jointure --file movies-ref.json --jsonArray
 * mongoimport -d moviesref -c jointure --file artists-ref.json --jsonArray
 */
class MongoMain {

    static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("localhost", 27017)
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        database.listCollectionNames().each {
            println it
        }

        MongoCollection<Document> collection = database.getCollection("artists")


        println collection.count()

        //coursMapReduce(database.getCollection("artists"))
        //exerciceS21(database.getCollection("artists"))
        //exerciceS22(mongoClient.getDatabase("moviesref").getCollection("jointure"))
        exerciceS23(mongoClient.getDatabase("nfe204").getCollection("movies"))
    }

    // http://b3d.bdpedia.fr/mapreduce.html#ex-s2-3
    private static void exerciceS23(MongoCollection<Document> moviesCollection) {
        String map = """
            function() {
             var tokens = this.title.match(/\\S+/g)
             for (var i = 0; i < tokens.length; i++) {
                var object = new Object()
                object.title = this.title
                object.summaryCount = this.summary.match(tokens[i]).length 
                emit(tokens[i], object);                
             }
            }
        """
        String reduce = """
            function(terme, mapResponse) {
                var res = new Object();
                res.terme = terme;
                res.titres = titres;
                res.summaryCount = 
                return res;
            }
        """

        moviesCollection.mapReduce(map, reduce).each {
            println it
        }
    }

    private static void exerciceS22(MongoCollection<Document> jointureCollection) {
        println jointureCollection.count()
        String map = """
            function() {
              // Est-ce que la clé du document contient le mot "artist"?
              if (this._id.indexOf("artist") != -1) {
                // Oui ! C'est un artiste. Ajoutons-lui son type.
                this.type="artist";
                // On produit une paire avec pour clé celle de l'artiste
                emit(this._id, this);
              } else {
                // Non: c'est un film. Ajoutons-lui son type.
               this.type="film";
               // Simplifions un peu le document pour l'affichage
               delete this.summary;
               delete this.actors;
                // On produit une paire avec pour clé celle du metteur en scène
               emit(this.director._id, this);
             }
            }
        """
        String reduce = """
            function(id, items) {
            
              var director = null, films={result: []}
            
              // Commençons par chercher l'artiste dans cette liste
              for (var idx = 0; idx < items.length; idx++) {
                if (items[idx].type=="artist") {
                     director = items[idx];
                 }
              }
            
              // Maintenant, 'director' contient l'artiste : on l'affecte aux films
              for (var idx = 0; idx < items.length; idx++) {
                 if (items[idx].type=="film"  && director != null) {
                     items[idx].director = director;
                     films.result.push (items[idx]);
                  }
               }
               return films;
             }
        """
        jointureCollection.mapReduce(map, reduce).each {
            println it
        }
    }

    private static void coursMapReduce(MongoCollection<Document> movies) {
        // MAP / REDUCE
        String map = """
            function() { 
                emit(this.director._id, this.title);
            }
        """

        String reduce = """
            function(directorId, titres) {
               var res = new Object();
               res.director = directorId;
               res.films = titres;
               return res;
             }
        """

        movies.mapReduce(map, reduce).each {
            println it
        }

        println "-----------------------"
        DBObject obj = new BasicDBObject()
        obj.put( "country", "USA" )
        movies.mapReduce(map, reduce).filter(obj).each {
            println it
        }
    }

    private static void exerciceS21(MongoCollection<Document> movies) {
        // select count(*) from movies
        // MAP / REDUCE
        String map = """
            function() { 
                emit('monCount', 1);
            }
        """

        String reduce = """
            function(id, set) {
               return set.length;
             }
        """

        movies.mapReduce(map, reduce).each {
            println it
        }

        // select count(*) from movies group by genre
        map = """
            function() { 
                emit(this.genre, 1);
            }
        """

        reduce = """
            function(genre, set) {
                return set.length;
             }
        """
        movies.mapReduce(map, reduce).each {
            println it
        }
    }
}
