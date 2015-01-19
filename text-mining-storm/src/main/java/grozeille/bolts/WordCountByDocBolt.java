package grozeille.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Mathias on 18/01/2015.
 */
public class WordCountByDocBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountByDocBolt.class);

    public static final String OUTPUT = "wordcountbydocbolt.output";

    private static final Pattern numberPattern = Pattern.compile("^[0-9]+$");

    private transient String outputFilePath;
    private transient FileOutputStream fos;
    private transient DataOutputStream dos;
    private transient OutputStreamWriter writer;

    private transient ObjectMapper objectMapper;
    private transient FrenchAnalyzer analyzer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        objectMapper = new ObjectMapper();
        analyzer = new FrenchAnalyzer();

        String outputPath = (String)stormConf.get(OUTPUT);
        try {
            File rootPath = new File(outputPath);
            if(!rootPath.exists()){
                rootPath.mkdirs();
            }

            outputFilePath = new File(outputPath, "countperdoc_" + context.getThisTaskId() + ".json").toPath().toString();

            fos = new FileOutputStream(outputFilePath);
            dos = new DataOutputStream(fos);
            writer = new OutputStreamWriter(dos);
            System.exit(-1);
        } catch (FileNotFoundException e) {
            LOG.error("Unable to write file", e);
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        HashMap<String, MutableInt> wordCount = new HashMap<>();

        LOG.info("AAA");

        String json = input.getString(0);
        TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};

        try {
            HashMap<String,Object> o = objectMapper.readValue(json, typeRef);
            String path = (String)o.get("path");
            String body = (String)o.get("body");

            if(body == null){
                return;
            }

            TokenStream tokenStream = analyzer.tokenStream("", new StringReader(body));
            OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                int startOffset = offsetAttribute.startOffset();
                int endOffset = offsetAttribute.endOffset();
                String term = charTermAttribute.toString();

                // skip les chaines avec uniquement des numéros
                if(numberPattern.matcher(term).matches() || term.length() < 2){
                    continue;
                }

                MutableInt count = wordCount.get(term.toUpperCase());
                if(count == null){
                    count = new MutableInt(0);
                    wordCount.put(term.toUpperCase(), count);
                }
                count.increment();
            }

            HashMap<String, Object> jsonData = new HashMap<>();
            jsonData.put("path", path);
            HashMap<String, Object> jsonWordCount = new HashMap<>();
            jsonData.put("count", jsonWordCount);

            for(Map.Entry<String, MutableInt> entries : wordCount.entrySet()){
                jsonWordCount.put(entries.getKey(), entries.getValue().getValue());
            }

            String jsonLine = objectMapper.writeValueAsString(jsonData);
            writer.append(jsonLine).append("\n");
            writer.flush();

            tokenStream.close();
        } catch (IOException e) {
            LOG.error("Unable to parse Json", e);
            collector.reportError(e);
        }
        catch (Exception e){
            LOG.error("Unable to parse Json", e);
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
