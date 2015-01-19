package grozeille;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.*;

/**
 * Created by Mathias on 17/01/2015.
 */
public class IndexToEs {
    private static final Logger LOG = LoggerFactory.getLogger(IndexToEs.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        Node node = nodeBuilder().node();
        Client client = node.client();
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
                    IndexResponse response = client.prepareIndex("documents", "document", map.get("path"))
                            .setSource(line)
                            .execute()
                            .actionGet();
                }
                catch (Exception ex){
                    LOG.error("Unable to index:\n"+line);
                }
            }
        }

        node.close();
    }
}
