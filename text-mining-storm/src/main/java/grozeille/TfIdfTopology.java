package grozeille;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import grozeille.bolts.TfidfByDocBolt;
import grozeille.bolts.WordCountAllBolt;
import grozeille.spouts.JsonFileSpout;

/**
 * Created by Mathias on 18/01/2015.
 */
public class TfIdfTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("json", new JsonFileSpout(), 1);

        builder.setBolt("count", new TfidfByDocBolt(), 1).shuffleGrouping("json");

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(JsonFileSpout.INPUT, "/Users/mathias/Work/test-amina/output-count");
        conf.put(TfidfByDocBolt.OUTPUT, "/Users/mathias/Work/test-amina/output-tfidf");
        conf.put(TfidfByDocBolt.WORDCOUNT, "/Users/mathias/Work/test-amina/output-count-all/allwords_2.json");
        conf.put(TfidfByDocBolt.MIN, 10);
        //conf.setMaxSpoutPending(8);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);

            StormSubmitter.submitTopologyWithProgressBar("wordcount-document", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(2);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount-document", conf, builder.createTopology());

            Thread.sleep(10*60*1000);

            cluster.shutdown();
        }
    }
}
