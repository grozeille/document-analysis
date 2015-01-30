package grozeille.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import grozeille.WordWithStat;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Created by Mathias on 18/01/2015.
 */
public class ClassifyBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ClassifyBolt.class);

    public static final String INPUT = "classify.input";
    public static final String CLASS_RESULT = "classify.clusteringResult";
    public static final String OUTPUT = "classify.output";

    private transient String outputFilePath;
    private transient FileOutputStream fos;
    private transient DataOutputStream dos;
    private transient OutputStreamWriter writer;
    private transient List<String> clusteringResult;

    private transient ObjectMapper objectMapper;
    private transient Map<String, List<Map<String, MutableInt>>> clusters;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        objectMapper = new ObjectMapper();
        clusteringResult = new ArrayList<>();

        clusters = new HashMap<>();

        String outputPath = (String)stormConf.get(OUTPUT);
        String classResultPath= (String)stormConf.get(CLASS_RESULT);

        try {

            BufferedInputStream is = new BufferedInputStream(new FileInputStream(classResultPath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = null;
            do{
                line = reader.readLine();
                clusteringResult.add(line);
            }while(line != null);
            reader.close();

            // création du fichier en sortie
            File rootPath = new File(outputPath);
            if(!rootPath.exists()){
                rootPath.mkdirs();
            }

            outputFilePath = new File(outputPath, "class_" + context.getThisTaskId() + ".json").toPath().toString();

            fos = new FileOutputStream(outputFilePath);
            dos = new DataOutputStream(fos);
            writer = new OutputStreamWriter(dos);

        } catch (FileNotFoundException e) {
            LOG.error("Unable to prepare", e);
            System.exit(-1);
        } catch (IOException e) {
            LOG.error("Unable to prepare", e);
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        HashMap<String, MutableInt> wordCount = new HashMap<>();

        String json = input.getString(0);

        if("flush".equals(json)){
            try {
                writer.flush();
            } catch (IOException e) {
                LOG.error("Unable to flush", e);
                collector.reportError(e);
            }
            LOG.info("Flush "+outputFilePath);
            return;
        }

        TypeReference<LinkedHashMap<String,Object>> typeRef = new TypeReference<LinkedHashMap<String,Object>>() {};

        try {
            HashMap<String,Object> o = objectMapper.readValue(json, typeRef);
            Map<String, Integer> count = (Map<String, Integer>)o.get("count");
            String path = (String)o.get("path");
/*
            List<Map<String, MutableInt>>

            for(Map.Entry<String, Object> e : o.entrySet()){
                String key = e.getKey();

            }


            for(WordWithStat word : wordcount){

                Integer numOfOccurrences = count.get(word.getWord());
                if(numOfOccurrences == null){
                    numOfOccurrences = 0;
                }
                Integer totalTermsInDocument = count.size();

                float tf = numOfOccurrences.floatValue() / (Float.MIN_VALUE + totalTermsInDocument.floatValue());
                float idf = (float) Math.log10(totalDocs.floatValue() / (Float.MIN_VALUE + word.getStats().getDocuments().floatValue()));
                float tfidf = (tf * idf);
                allTfidf.add(tfidf);
            }


            writer.append(path);
            for(Float tfidf : allTfidf){
                writer.append("\t").append(tfidf.toString());
            }
            writer.append("\n");
            writer.flush();
*/
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
        if(fos != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Unable to write file", e);
            }
        }
    }
}
