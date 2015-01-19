package grozeille;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import grozeille.bolts.DocumentExtractorBolt;
import grozeille.bolts.JsonWriterBolt;
import grozeille.spouts.LocalFileSpout;

import java.io.IOException;

/**
 * Created by Mathias on 18/01/2015.
 */
public class ParseDocumentTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new LocalFileSpout(), 1);

        builder.setBolt("extract", new DocumentExtractorBolt(), 4).shuffleGrouping("spout");
        builder.setBolt("write", new JsonWriterBolt(), 2).shuffleGrouping("extract");

        Config conf = new Config();
        conf.setDebug(false);
        //conf.put(LocalFileSpout.INPUT, "Z:\\AMINA\\03 A ranger");
        conf.put(LocalFileSpout.INPUT, "/Volumes/data/AMINA/03 A ranger");
        conf.put(LocalFileSpout.EXTENSIONS, "pdf");
        //conf.put(JsonWriterBolt.OUTPUT, "C:\\Users\\Mathias\\Documents\\aranger-output2");
        conf.put(JsonWriterBolt.OUTPUT, "/Users/mathias/Work/test-amina/output-parsed");
        conf.setMaxSpoutPending(8);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(4);

            StormSubmitter.submitTopologyWithProgressBar("parse-document", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(4);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("parse-document", conf, builder.createTopology());

            Thread.sleep(10*60*1000);

            cluster.shutdown();
        }
    }
}
