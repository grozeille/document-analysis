package grozeille;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by Mathias on 17/01/2015.
 */
public class IndexToSolr {
    private static final Logger LOG = LoggerFactory.getLogger(IndexToSolr.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        SolrServer server = new HttpSolrServer("http://localhost:8983/solr/");
        ObjectMapper objectMapper = new ObjectMapper();

        File file = new File("C:\\Users\\Mathias\\Documents\\aranger-output\\output_1_0.json");
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            Map<String,String> map = new HashMap<String,String>();

            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }


                map = objectMapper.readValue(line, new TypeReference<HashMap<String,String>>(){});

                try {
                    SolrInputDocument doc = new SolrInputDocument();
                    for(Map.Entry<String, String> entry : map.entrySet()){
                        doc.addField(entry.getKey()+"_fr", entry.getValue());
                    }
                    doc.addField("id", map.get("path"));

                    server.add( doc );
                    server.commit();
                }
                catch (Exception ex){
                    LOG.error("Unable to index:\n"+line);
                }
            }
        }
    }
}
