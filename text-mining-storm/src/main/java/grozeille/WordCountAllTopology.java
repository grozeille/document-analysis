package grozeille;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import grozeille.bolts.WordCountAllBolt;
import grozeille.bolts.WordCountByDocBolt;
import grozeille.spouts.JsonFileSpout;

/**
 * Created by Mathias on 18/01/2015.
 */
public class WordCountAllTopology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("json", new JsonFileSpout(), 1);

        builder.setBolt("count", new WordCountAllBolt(), 1).shuffleGrouping("json");

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(JsonFileSpout.INPUT, "/Users/mathias/Work/test-amina/output-count");
        conf.put(WordCountAllBolt.OUTPUT, "/Users/mathias/Work/test-amina/output-count-all2");
        //conf.setMaxSpoutPending(8);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);

            StormSubmitter.submitTopologyWithProgressBar("wordcount-document", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(2);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount-document", conf, builder.createTopology());

            LocalDRPC drpc = new LocalDRPC();


            Thread.sleep(10*60*1000);

            cluster.shutdown();
        }
    }
}
