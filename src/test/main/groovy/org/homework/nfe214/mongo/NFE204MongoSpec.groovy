package org.homework.nfe214.mongo

import com.mongodb.MongoClient
import com.mongodb.client.MapReduceIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document
import spock.lang.Shared
import spock.lang.Specification

class NFE204MongoSpec extends Specification {

    @Shared
    MongoClient mongoClient

    def setupSpec() {
        mongoClient = new MongoClient("localhost", 27017)
    }

    def cleanupSpec() {
        mongoClient.close()
    }

    def "Simple connection à la base de données nfe204"() {
        when:
        MongoDatabase database = mongoClient.getDatabase("nfe204")

        then:
        database != null
        database.listCollectionNames().size() == 1
        database.listCollectionNames().contains("movies")
    }

    def "Comptage du nombre d'artist dans la base de données nfe204"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")

        when:
        int count = database.getCollection("movies").count()

        then:
        count == 88
    }

    def "Mon premier map/reduce sur la base de données nfe204"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")

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

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.size() == 50
        result.each {
            println it
        }
    }

    def "Exercice S21 - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-1 - variante 1 - select count(*) from movies"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")

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

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.size() == 1
        result[0].value == 88
    }

    def "Exercice S21 - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-1 - variante 2 - select count(*) from movies group by genre"() {
        setup:
        def control = [Action:13,Comédie:4,Fantastique:2,Guerre:4,Horreur:4,"Science-fiction":13, Suspense:2, Thriller:7, Western:5, crime:11, drama:22,romance:1]

        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")

        String map = """
            function() { 
                emit(this.genre, 1);
            }
        """

        String reduce = """
            function(genre, set) {
                return set.length;
             }
        """

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.size() == control.size()
        result.each {
            assert it.value == control[it._id]
        }
    }

    def "Exercice S22 - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-2"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("moviesref")
        MongoCollection<Document> moviesCollection = database.getCollection("jointure")

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
        // ATTENTION : MongoDB n’appelle pas la fonction reduce() pour des groupes contenant un seul document.
        String reduce = """
            function(id, items) {
            
              var director = null, films={result: []}
            
              // Commençons par chercher l'artiste dans cette liste
              for (var idx = 0; idx < items.length; idx++) {
                if (items[idx].type=="artist") {
                     director = items[idx];
                 }
              }
              
              
              var result = new Object()
              result.director = director
              // Maintenant, 'director' contient l'artiste : on l'affecte aux films
              for (var idx = 0; idx < items.length; idx++) {
                 if (items[idx].type=="film"  && director != null) {
                     items[idx].director = director;
                     films.result.push (items[idx]);
                  }
               }
               result.films = films
               return result;
             }
        """

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }
    def "Exercice S22 - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-2 - sur les films francais avant 2000"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("moviesref")
        MongoCollection<Document> moviesCollection = database.getCollection("jointure")

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
               if(this.country == "FR" && this.year <= 2000) {
                emit(this.director._id, this);
               }
             }
            }
        """
        // ATTENTION : MongoDB n’appelle pas la fonction reduce() pour des groupes contenant un seul document.
        String reduce = """
            function(id, items) {
            
              var director = null, films={result: []}
            
              // Commençons par chercher l'artiste dans cette liste
              for (var idx = 0; idx < items.length; idx++) {
                if (items[idx].type=="artist") {
                     director = items[idx];
                 }
              }
              
              
              var result = new Object()
              result.director = director
              // Maintenant, 'director' contient l'artiste : on l'affecte aux films
              for (var idx = 0; idx < items.length; idx++) {
                 if (items[idx].type=="film"  && director != null) {
                     items[idx].director = director;
                     films.result.push (items[idx]);
                  }
               }
               result.films = films
               return result;
             }
        """

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "S2-3 - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-3 - Comptage des mots dans les résumés"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")
        String map = """
            function() {
             var tokens = this.title.match(/\\S+/g);
             for (var i = 0; i < tokens.length; i++) {
                var object = new Object();
                object.title = this.title;
                object.summaryCount = 0;
                if(this.summary != null) {
                    var re = new RegExp(tokens[i],"gi")
                    var result = this.summary.match(re);
                    if(result != null) {
                        object.summaryCount = result.length;
                    } 
                }
                emit(tokens[i].toLowerCase(), object);                
             }
            }
        """
        String reduce = """
            function(terme, mapResponse) {
                var object = new Object();
                object.response = mapResponse;
                return object;
            }
        """

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "S2-3 - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-3 - Comptage des mots dans les résumés sommés"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")
        String map = """
            function() {
             var tokens = this.title.match(/\\S+/g);
             for (var i = 0; i < tokens.length; i++) {
                var object = new Object();
                object.title = this.title;
                object.summaryCount = 0;
                if(this.summary != null) {
                    var re = new RegExp(tokens[i],"gi");
                    var result = this.summary.match(re);
                    if(result != null) {
                        object.summaryCount = result.length;
                    } 
                }
                emit(tokens[i].toLowerCase(), object);                
             }
            }
        """
        String reduce = """
            function(terme, mapResponses) {
                var sum = 0;
                for (var i = 0; i < mapResponses.length; i++) {
                    sum = sum + mapResponses[i].summaryCount;
                }
                
                var object = new Object();
                object.sum = sum;
                object.response = mapResponses;
                return object;
            }
        """

        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }


    def "S2-4 Classification de document - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-4 - Un classement par genre"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")
        String map = """
            function() {
                emit(this.genre, this);
            }
        """
        String reduce = """
            function(genre, movies) {
               var films={result: []};
               for(var i =0; i < movies.length; i++) {
                films.result.push(movies[i].title);
               }
               return films;
            }    
        """
        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }

    def "S2-4 Classification de document - http://b3d.bdpedia.fr/mapreduce.html#ex-s2-4 - Un classement par décennie"() {
        setup:
        MongoDatabase database = mongoClient.getDatabase("nfe204")
        MongoCollection<Document> moviesCollection = database.getCollection("movies")
        String map = """
            function() {
                var decennie = this.year - (this.year % 10)
                emit(decennie, this);
            }
        """
        String reduce = """
            function(genre, movies) {
               var films={result: []};
               for(var i =0; i < movies.length; i++) {
                films.result.push(movies[i].title);
               }
               return films;
            }    
        """
        when:
        MapReduceIterable<?> result = moviesCollection.mapReduce(map, reduce)

        then:
        result != null
        result.each {
            println it
        }
    }
}
