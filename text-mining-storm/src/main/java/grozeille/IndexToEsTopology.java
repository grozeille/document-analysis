package grozeille;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import grozeille.bolts.IndexEsBolt;
import grozeille.bolts.WordCountAllBolt;
import grozeille.spouts.JsonFileSpout;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import static org.elasticsearch.node.NodeBuilder.*;
/**
 * Created by Mathias on 18/01/2015.
 */
public class IndexToEsTopology {
    private static final Logger LOG = LoggerFactory.getLogger(IndexToEsTopology.class);

    public static void main(String[] args) throws Exception {

        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));


        File file = new File("/Users/mathias/Work/test-amina/KMeans2_abc1376ec08e39a3c1b85a0982f4c46_clusters.csv");

        List<String> kmeans = IOUtils.readLines(new FileReader(file));

        int cpt = 0;

        file = new File("/Users/mathias/Work/test-amina/output-tfidf/tfidf_2.csv");
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {


            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                if(cpt == 0){
                    cpt++;
                    continue;
                }

                String clusterId = kmeans.get(cpt).replace("\"", "");

                String path = line.substring(0, line.indexOf("\t"));


                try {
                    client.prepareUpdate("documents", "document", path)
                            .setUpsert(XContentFactory.jsonBuilder().startObject().field("cluster", 1).endObject())
                            .setScript("ctx._source.cluster = " + clusterId, ScriptService.ScriptType.INLINE)
                            .execute()
                            .actionGet();
                }
                catch (Exception ex){
                    LOG.error("Unable to index:\n"+line, ex);
                }

                cpt++;


            }
        }

        client.close();

        LOG.info("End");

        System.exit(0);

        /********/


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("json", new JsonFileSpout(), 1);

        builder.setBolt("index", new IndexEsBolt(), 1).shuffleGrouping("json");

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(JsonFileSpout.INPUT, "/Users/mathias/Work/test-amina/output-parsed2");
        conf.put(IndexEsBolt.HOST, "");
        conf.put(IndexEsBolt.INDEX, "");
        //conf.setMaxSpoutPending(8);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);

            StormSubmitter.submitTopologyWithProgressBar("index", conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(2);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("index", conf, builder.createTopology());

            Thread.sleep(10*60*1000);

            cluster.shutdown();
        }
    }
}
