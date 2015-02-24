package grozeille;

import com.lucidworks.spark.SolrRDD;
import org.apache.commons.cli.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.tika.language.LanguageIdentifier;
import scala.Tuple3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Created by Mathias on 16/02/2015.
 */
public class TrainModelProgram {

    public static void main(String[] args) throws SolrServerException, IOException {

        Options options = new Options();


        options.addOption(OptionBuilder
                .withLongOpt("output")
                .withDescription("output folder for model")
                .hasArg()
                .isRequired()
                .create("o"));
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
                .withLongOpt("clusterNumber")
                .withDescription("number of cluster to generate")
                .hasArg()
                .isRequired()
                .create("n"));
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

        File clusterDirectory = new File(cmd.getOptionValue("o"));
        String collection = cmd.getOptionValue("c");
        Integer numCluster = Integer.parseInt( cmd.getOptionValue("n"));
        String zkHosts = cmd.getOptionValue("zk", "localhost:9983");

        LanguageIdentifier.initProfiles();
        /*System.out.println("Supported languages:");
        LanguageIdentifier.getSupportedLanguages().stream().forEach(s -> System.out.println(s));*/

        SparkConf conf = new SparkConf().setAppName("text-mining.train");
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

        // compute groups
        KMeansModel clusters = SparkPipeline.clustering(clusterDirectory, numCluster, documents, zkHosts, collection);

        sc.stop();

        try {
            if(!clusterDirectory.exists()){
                clusterDirectory.mkdir();
            }

            FileOutputStream fileOut = new FileOutputStream(new File(clusterDirectory, "kmeansmodel.ser").toString());
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(clusters);
            out.close();
            fileOut.close();
        }catch(IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
