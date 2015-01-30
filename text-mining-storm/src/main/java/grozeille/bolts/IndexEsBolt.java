package grozeille.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.*;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by mathias on 26/01/2015.
 */
public class IndexEsBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(JsonWriterBolt.class);

    public static final String HOST = "indexesbolt.host";
    public static final String INDEX = "indexesbolt.index";

    private transient ObjectMapper objectMapper;
    private transient String host;
    private transient String index;
    private transient Client client;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        host = (String)stormConf.get(HOST);
        index = (String)stormConf.get(INDEX);

        client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        objectMapper = new ObjectMapper();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String json = input.getString(0);

        if("flush".equals(json)){
            client.close();
            LOG.info("Flush ");
            return;
        }

        TypeReference<LinkedHashMap<String,Object>> typeRef = new TypeReference<LinkedHashMap<String,Object>>() {};

        Map<String,String> map = null;
        try {
            map = objectMapper.readValue(input.getString(0), typeRef);
        } catch (IOException e) {
            LOG.error("Unable to parse JSON", e);
            collector.reportError(e);
            return;
        }

        try {
            IndexResponse response = client.prepareIndex("documents", "document", map.get("path"))
                    .setSource(input.getString(0))
                    .execute()
                    .actionGet();
        }
        catch (Exception e){
            LOG.error("Unable to index", e);
            collector.reportError(e);
            return;
        }
    }

    @Override
    public void cleanup() {
        client.close();
    }
}