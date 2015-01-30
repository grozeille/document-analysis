package grozeille.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Ordering;
import grozeille.WordStat;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by Mathias on 18/01/2015.
 */
public class WordCountAllBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountAllBolt.class);

    public static final String OUTPUT = "wordcountbolt.output";

    private transient String outputFilePath;

    private transient ObjectMapper objectMapper;

    private transient Map<String, WordStat> totalCount;
    private transient MutableInt totalDoc;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        objectMapper = new ObjectMapper();

        String outputPath = (String)stormConf.get(OUTPUT);
        File rootPath = new File(outputPath);
        if(!rootPath.exists()){
            rootPath.mkdirs();
        }

        outputFilePath = new File(outputPath, "allwords_" + context.getThisTaskId() + ".json").toPath().toString();

        totalCount = new HashMap<>();
        totalDoc = new MutableInt();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        HashMap<String, MutableInt> wordCount = new HashMap<>();
        totalDoc.increment();

        String json = input.getString(0);

        if("flush".equals(json)){

            FileOutputStream fos = null;
            OutputStreamWriter writer = null;

            try {

                List<Map.Entry<String, WordStat>> wordByCount = Ordering.from(new Comparator<Map.Entry<String, WordStat>>() {
                    @Override
                    public int compare(Map.Entry<String, WordStat> o1, Map.Entry<String, WordStat> o2) {
                        return new Integer(o1.getValue().getCount().intValue()).compareTo(new Integer(o2.getValue().getCount().intValue()));
                    }
                }).reverse().sortedCopy(totalCount.entrySet());

                fos = new FileOutputStream(outputFilePath);
                writer = new OutputStreamWriter(new DataOutputStream(fos));

                Map<String, Object> rootObject = new HashMap<>();
                rootObject.put("totalDocs", totalDoc.intValue());
                Map<String, Object> wordsObject = new LinkedHashMap<>();
                rootObject.put("words", wordsObject);

                for(Map.Entry<String, WordStat> entry : wordByCount){

                    Map<String, Integer> wordStats = new HashMap<>();
                    wordStats.put("documents", entry.getValue().getDocuments().intValue());
                    wordStats.put("count", entry.getValue().getCount().intValue());

                    wordsObject.put(entry.getKey(), wordStats);

                }

                objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, false);
                writer.write(objectMapper.writeValueAsString(rootObject));

                writer.flush();
            } catch (IOException e) {
                LOG.error("Unable to parse Json\n"+json, e);
                collector.reportError(e);
            }
            finally {
                if(fos != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        LOG.error("Unable to write file", e);
                    }
                }
            }

            LOG.info("Flush "+outputFilePath);

            return;
        }


        totalDoc.increment();

        TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};

        try {
            HashMap<String,Object> o = objectMapper.readValue(json, typeRef);
            String path = (String)o.get("path");
            Map<String, Integer> count = (Map<String, Integer>)o.get("count");

            for(Map.Entry<String, Integer> entry : count.entrySet()) {

                WordStat wordStat = totalCount.get(entry.getKey().toUpperCase());
                if(wordStat == null){
                    wordStat = new WordStat();
                    totalCount.put(entry.getKey().toUpperCase(), wordStat);
                }
                wordStat.getDocuments().increment();
                wordStat.getCount().add(entry.getValue());
            }

        } catch (IOException e) {
            LOG.error("Unable to parse Json\n"+json, e);
            collector.reportError(e);
        }
        catch (Exception e){
            LOG.error("Unable to parse Json\n"+json, e);
            collector.reportError(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
