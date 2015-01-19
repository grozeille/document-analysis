package grozeille.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by Mathias on 18/01/2015.
 */
public class JsonWriterBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(JsonWriterBolt.class);

    public static final String OUTPUT = "jsonwriterbolt.output";

    private transient String outputFilePath;
    private int cpt = 0;
    private transient ObjectMapper objectMapper;
    private transient FileOutputStream fos;
    private transient DataOutputStream dos;
    private transient OutputStreamWriter writer;
    private transient String outputPath;
    private transient int taskId;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        outputPath = (String)stormConf.get(OUTPUT);

        taskId = context.getThisTaskId();
        objectMapper = new ObjectMapper();

        try {
            File rootPath = new File(outputPath);
            if(!rootPath.exists()){
                rootPath.mkdirs();
            }

            outputFilePath = new File(outputPath, "output_" + taskId + "_" + cpt + ".json").toPath().toString();
            fos = new FileOutputStream(outputFilePath);
            dos = new DataOutputStream(fos);
            writer = new OutputStreamWriter(dos);
        } catch (FileNotFoundException e) {
            LOG.error("Unable to write file", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            writer.append(objectMapper.writeValueAsString(input.getValue(0)));
            writer.append("\n");

            if(dos.size() > 1024 * 1024 * 64){
            //if (dos.size() > 1024 * 1024 * 1) {

                LOG.info("Max size of file reached, create new file");

                try {
                    writer.close();
                } catch (IOException e) {
                    LOG.error("Unable to write file", e);
                }
                cpt++;
                outputFilePath = new File(outputPath, "output_" + taskId + "_" + cpt + ".json").toPath().toString();
                fos = new FileOutputStream(outputFilePath);
                dos = new DataOutputStream(fos);
                writer = new OutputStreamWriter(dos);
            }
        } catch (JsonProcessingException e) {
            LOG.error("Unable to write JSON", e);
            collector.reportError(e);
        } catch (FileNotFoundException e) {
            LOG.error("Unable to write JSON", e);
            collector.reportError(e);
        } catch (IOException e) {
            LOG.error("Unable to write JSON", e);
            collector.reportError(e);
        }
    }

    @Override
    public void cleanup() {
        if(fos != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Unable to write file", e);
            }
        }
    }
}