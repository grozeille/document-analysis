package grozeille.workers;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import grozeille.FileItem;
import grozeille.framework.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Mathias on 11/01/2015.
 */
public class JsonWriterWorker extends Worker<Map<String, Object>, Object> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonWriterWorker.class);

    private final String outputPath;
    private int cpt;
    private ObjectMapper objectMapper;
    private String outputFilePath;
    private FileOutputStream fos;
    private DataOutputStream dos;
    private OutputStreamWriter writer;

    public JsonWriterWorker(int workerId, MetricRegistry registry, BlockingQueue<Map<String, Object>> input, String outputPath) {
        super(workerId, registry, input, Arrays.asList(new ArrayBlockingQueue[0]));
        this.outputPath = outputPath;

    }

    @Override
    protected void init() throws Exception {
        super.init();
        cpt = 0;
        objectMapper = new ObjectMapper();
        outputFilePath = outputPath+"\\output_"+this.getTaskId()+"_"+ cpt +".json";
        fos = new FileOutputStream(outputFilePath);
        dos = new DataOutputStream(fos);
        writer = new OutputStreamWriter(dos);
    }

    @Override
    protected void shutdown() {
        super.shutdown();
        if(fos != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Unable to write file", e);
            }
        }
    }

    @Override
    protected Object processItem(Map<String, Object> item) throws Exception {
        writer.append(objectMapper.writeValueAsString(item));
        writer.append("\n");

        //if(dos.size() > 1024 * 1024 * 64){
        if(dos.size() > 1024 * 1024 * 1){

            LOG.info("Max size of file reached, create new file");

            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Unable to write file", e);
            }
            cpt++;
            outputFilePath = outputPath+"\\output_"+this.getTaskId()+"_"+cpt+".json";
            fos = new FileOutputStream(outputFilePath);
            dos = new DataOutputStream(fos);
            writer = new OutputStreamWriter(dos);
        }

        return null;
    }
}
