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
public class TfidfByDocBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TfidfByDocBolt.class);

    public static final String OUTPUT = "tfidf.output";
    public static final String WORDCOUNT = "tfidf.wordcount";
    public static final String MIN = "tfidf.min";

    private transient String outputFilePath;
    private transient FileOutputStream fos;
    private transient DataOutputStream dos;
    private transient OutputStreamWriter writer;
    private transient List<WordWithStat> wordcount;
    private transient Integer totalDocs;

    private transient ObjectMapper objectMapper;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        objectMapper = new ObjectMapper();
        wordcount = new ArrayList<>();

        String outputPath = (String)stormConf.get(OUTPUT);
        String wordcountPath= (String)stormConf.get(WORDCOUNT);
        Long minSize = (Long)stormConf.get(MIN);
        if(minSize == null){
            minSize = 10l;
        }
        try {

            // lecture du wordcount
            BufferedInputStream is = new BufferedInputStream(new FileInputStream(wordcountPath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = reader.readLine();
            TypeReference<LinkedHashMap<String,Object>> wordcountMapType = new TypeReference<LinkedHashMap<String,Object>>() {};

            Map<String, Object> allStats = (Map<String, Object>)objectMapper.readValue(line, wordcountMapType);
            totalDocs = (Integer)allStats.get("totalDocs");
            Map<String, Object> wordStats = (Map<String, Object>)allStats.get("words");
            int cpt = 0;
            LOG.info("Total words: "+wordStats.size());
            for(Map.Entry<String, Object> entry : wordStats.entrySet()){
                Integer documents = ((Map<String, Integer>)entry.getValue()).get("documents");
                Integer count = ((Map<String, Integer>)entry.getValue()).get("count");

                if(count >= minSize) {
                    WordWithStat wordWithStat = new WordWithStat();
                    wordWithStat.setWord(entry.getKey());
                    wordWithStat.getStats().getDocuments().setValue(documents);
                    wordWithStat.getStats().getCount().setValue(count);

                    wordcount.add(wordWithStat);
                }
                cpt++;
                if(cpt >= 6000){
                    break;
                }
            }
            reader.close();

            // création du fichier en sortie
            File rootPath = new File(outputPath);
            if(!rootPath.exists()){
                rootPath.mkdirs();
            }

            outputFilePath = new File(outputPath, "tfidf_" + context.getThisTaskId() + ".csv").toPath().toString();

            fos = new FileOutputStream(outputFilePath);
            dos = new DataOutputStream(fos);
            writer = new OutputStreamWriter(dos);

            writer.append("path");
            for(WordWithStat w : wordcount){
                writer.append("\t").append(w.getWord());
            }
            writer.append("\n");
            writer.flush();

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

            List<Float> allTfidf = new ArrayList<>();

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
