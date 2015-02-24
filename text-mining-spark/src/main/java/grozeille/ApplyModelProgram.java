package grozeille;

import com.lucidworks.spark.SolrRDD;
import org.apache.commons.cli.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.tika.language.LanguageIdentifier;
import scala.Tuple3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Created by Mathias on 16/02/2015.
 */
public class ApplyModelProgram {

    public static void main(String[] args) throws SolrServerException, IOException {

        Options options = new Options();


        options.addOption(OptionBuilder
                .withLongOpt("cluster")
                .withDescription("cluster model path")
                .hasArg()
                .isRequired()
                .create("i"));
        options.addOption(OptionBuilder
                .withLongOpt("output1")
                .withDescription("output folder for classified documents")
                .hasArg()
                .create("o1"));
        options.addOption(OptionBuilder
                .withLongOpt("output2")
                .withDescription("output folder for top words per class")
                .hasArg()
                .create("o2"));
        options.addOption(OptionBuilder
                .withLongOpt("zkHosts")
                .withDescription("solr zookeeper hosts to read. Default localhost:9983")
                .hasArg()
                .create("zk"));
        options.addOption(OptionBuilder
                .withLongOpt("collection")
                .withDescription("solr collection to read")
                .hasArg()
                .isRequired()
                .create("c"));
        options.addOption(OptionBuilder
                .withLongOpt("help")
                .withDescription("Help")
                .create("h"));
        options.addOption(OptionBuilder
                .withLongOpt("master")
                .withDescription("Spark master. Default none.")
                .hasArg()
                .create("m"));

        HelpFormatter formatter = new HelpFormatter();

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if(cmd.hasOption("h")){
                formatter.printHelp( "program", options );
            }
        } catch (ParseException e) {
            System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
            formatter.printHelp( "program", options );
            System.exit(-1);
        }

        File clusterPath = new File(cmd.getOptionValue("i"));
        File classifiedDocumentsDirectory = new File(cmd.getOptionValue("o1"));
        File topWordsDirectory = new File(cmd.getOptionValue("o2"));
        String collection = cmd.getOptionValue("c");
        String zkHosts = cmd.getOptionValue("zk", "localhost:9983");

        KMeansModel clusters = null;
        try
        {
            FileInputStream fileIn = new FileInputStream(clusterPath);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            clusters = (KMeansModel) in.readObject();
            in.close();
            fileIn.close();
        }catch(IOException i)
        {
            i.printStackTrace();
            System.exit(-1);
        }catch(ClassNotFoundException c)
        {
            System.out.println("KMeansModel class not found");
            c.printStackTrace();
            System.exit(-1);
        }

        SparkConf conf = new SparkConf().setAppName("text-mining.apply");
        if(cmd.hasOption("m")){
            conf.setMaster(cmd.getOptionValue("m"));
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        SolrRDD solrRDD = new SolrRDD(zkHosts, collection);
        //SolrQuery query = new SolrQuery("-content:[* TO *] AND  mimeType_s:\"application/pdf\"");
        SolrQuery query = new SolrQuery("mimeType_s:\"application/pdf\"");
        query.addSort("id", SolrQuery.ORDER.desc);
        query.addField("content");
        query.addField("id");

        JavaRDD<SolrDocument> solrJavaRDD = solrRDD.queryDeep(sc, query);

        // load all documents
        JavaRDD<Tuple3<String, String, String>> documents = solrJavaRDD.map((document) -> {
            Object content = document.getFieldValue("content");
            if(content == null){
                return null;
            }
            else {

                return new Tuple3<>(document.getFieldValue("id").toString(), content.toString(), new LanguageIdentifier( content.toString()).getLanguage());
            }
        }).filter((e) -> e != null);

        // classify documents with this model
        JavaPairRDD<String, Integer> documentsWithGroup = SparkPipeline.classify(classifiedDocumentsDirectory, documents, clusters, zkHosts, collection);

        // extract top words for all groups
        SparkPipeline.wordsPerCluster(topWordsDirectory, documents, documentsWithGroup, 50);

        sc.stop();

        CloudSolrServer solrServer = new CloudSolrServer(zkHosts);
        solrServer.setDefaultCollection(collection);
        solrServer.commit();

        SparkUtils.mergeFiles(classifiedDocumentsDirectory, "result.txt");
        SparkUtils.mergeFiles(topWordsDirectory, "result.txt");
    }
}
