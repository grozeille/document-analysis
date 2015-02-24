package grozeille;

import com.google.common.collect.Lists;
import com.lucidworks.spark.SolrSupport;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Mathias on 23/02/2015.
 */
public class SparkPipeline {

    private static final Pattern alphaPattern = Pattern.compile(".*\\p{Alpha}.*");

    public static KMeansModel clustering(
            File clusterDirectory,
            Integer numCluster,
            JavaRDD<Tuple3<String, String, String>> documents,
            final String zkHosts,
            final String collection) throws IOException {

        JavaRDD<Tuple2<String, Vector>> tfByDocument = tf(documents);

        // create flat RDD with all vectors
        JavaRDD<Vector> tf = tfByDocument.map((e) -> e._2());
        tf.cache();

        // create idf model
        IDFModel idf = new IDF().fit(tf);
        JavaRDD<Vector> tfIdf = idf.transform(tf);
        tfIdf.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = numCluster;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(tfIdf.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(tfIdf.rdd());
        System.out.println("***** Within Set Sum of Squared Errors = " + WSSSE);

        return clusters;
    }

    public static JavaRDD<Tuple2<String, Vector>> tf(JavaRDD<Tuple3<String, String, String>> documents) throws IOException {
        HashingTF hashingTF = new HashingTF(10_000);

        // Hash all documents
        JavaRDD<Tuple2<String, Vector>> tfByDocument = documents.map((tuple2) -> {
            String stringContent = tuple2._2().replace("_", " ").replace(".pdf", "");


            Analyzer analyzer = AnalyzerUtils.getAnalyzerByLanguage(tuple2._3());

            // filter small words and do shingle
            List<String> documentWords = Lists.newArrayList(new TokenStreamIterable(stringContent, analyzer, tokenStream -> {
                tokenStream = new LengthFilter(tokenStream, 2, 255);
                tokenStream = new PatternTokenFilter(tokenStream, alphaPattern);
                //tokenStream = new ShingleFilter(tokenStream, 1, 3);
                return tokenStream;
            }));
            if (documentWords.isEmpty()) {
                return null;
            }
            return new Tuple2<>(tuple2._1(), hashingTF.transform(documentWords));
        }).filter((e) -> e != null);

        return tfByDocument;
    }

    public static JavaPairRDD<String, Integer> classify(
            File classifiedDocumentsDirectory,
            JavaRDD<Tuple3<String, String, String>> documents,
            KMeansModel clusters,
            final String zkHosts,
            final String collection) throws IOException {

        JavaRDD<Tuple2<String, Vector>> tfByDocument = tf(documents);

        // write classification of documents
        JavaPairRDD<String, Integer> documentsWithGroup = tfByDocument.mapToPair((e) -> new Tuple2<>(e._1(), clusters.predict(e._2())));
        documentsWithGroup.map((e) -> e._2() + "\t" + e._1()).saveAsTextFile(classifiedDocumentsDirectory.toString());

        // update Solr
        /*JavaRDD<SolrInputDocument> solrInputDocs = documentsWithGroup.map(t -> {
            SolrInputDocument inputDoc = new SolrInputDocument();

            Map<String, String> setClusterId = new HashMap<>();
            setClusterId.put("set", t._2().toString());

            inputDoc.setField("id", t._1());
            inputDoc.setField("cluster_id_ti", setClusterId);

            return inputDoc;
        });

        int batchSize = 50;
        solrInputDocs.foreachPartition(solrInputDocumentIterator -> {

            CloudSolrServer solrServer = new CloudSolrServer(zkHosts);
            solrServer.setDefaultCollection(collection);

            List<SolrInputDocument> batch = new ArrayList<>();
            Date indexedAt = new Date();
            while (solrInputDocumentIterator.hasNext()) {
                SolrInputDocument inputDoc = solrInputDocumentIterator.next();
                inputDoc.setField("_indexed_at_tdt", indexedAt);
                batch.add(inputDoc);
                if (batch.size() >= batchSize) {
                    try {
                        SolrSupport.sendBatchToSolr(solrServer, collection, batch);
                    } catch (Exception e) {
                        e.printStackTrace();
                        batch.stream().forEach(item -> System.out.println(item.toString()));
                    }
                }
            }
            try {
                if (!batch.isEmpty()) {
                    SolrSupport.sendBatchToSolr(solrServer, collection, batch);
                }
            }catch (Exception e){
                e.printStackTrace();
                batch.stream().forEach(item -> System.out.println(item.toString()));
            }
        });*/

        return documentsWithGroup;
    }

    public static void wordsPerCluster(
            File topWordsDirectory,
            JavaRDD<Tuple3<String, String, String>> documents,
            JavaPairRDD<String, Integer> documentsWithGroup,
            final int topWords) throws IOException {

        // extract full words
        JavaPairRDD<String, List<String>> wordsByDocument = documents.mapToPair((tuple2) -> {
            String stringContent = tuple2._2().replace("_", " ").replace(".pdf", "");

            ClassicAnalyzer analyzer = new ClassicAnalyzer(AnalyzerUtils.getAnalyzerByLanguage(tuple2._3()).getStopwordSet());

            // filter small words and do shingle
            List<String> documentWords = Lists.newArrayList(new TokenStreamIterable(stringContent, analyzer, tokenStream -> {
                tokenStream = new LengthFilter(tokenStream, 2, 255);
                tokenStream = new PatternTokenFilter(tokenStream, alphaPattern);
                //tokenStream = new ShingleFilter(tokenStream, 1, 3);
                return tokenStream;
            }));

            if (documentWords.isEmpty()) {
                return null;
            }
            return new Tuple2<>(tuple2._1(), documentWords);
        }).filter((e) -> e != null);

        // join by document the clustering group and the words
        JavaPairRDD<String, Tuple2<Integer, List<String>>> groupAndWordsByDocument = documentsWithGroup.join(wordsByDocument);

        // flat map words with clustering group
        JavaRDD<String> wordsWithClusteringGroup = groupAndWordsByDocument.flatMap(t -> {
            // concat the group and the word
            return t._2()._2().stream()
                    .map((word) -> t._2()._1() + "\t" + word)
                    .collect(Collectors.toList());
        });//.groupBy((s) -> s.split("\t")[0]);

        JavaRDD<Tuple2<String, List<Tuple2<String, Integer>>>> clusteringGroupWithTopWords = wordsWithClusteringGroup
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum)
                .groupBy(t -> t._1().split("\t")[0])
                .map(t -> {
                    List<Tuple2<String, Integer>> wordsWithCount = IteratorUtils.toList(t._2().iterator());
                    // the 10th words most used
                    List<Tuple2<String, Integer>> topWordsWithCount = wordsWithCount.stream()
                            .map((tt) -> {
                                try {
                                    String word = tt._1().split("\t")[1];
                                    return new Tuple2<>(word, tt._2());
                                }catch (Exception e){
                                    e.printStackTrace();
                                    return null;
                                }
                            })
                            .sorted((tt1, tt2) -> -tt1._2().compareTo(tt2._2()))
                            .limit(topWords)
                            .collect(Collectors.toList());

                    return new Tuple2<>(t._1(), topWordsWithCount);
                });

        // save
        clusteringGroupWithTopWords.map(t -> {
            List<String> wordCount = t._2.stream().map(wc -> wc._1() + ":" + wc._2()).collect(Collectors.toList());

            return t._1() + "\t" + StringUtils.join(wordCount, "\t");
        }).saveAsTextFile(topWordsDirectory.toString());
    }
}
