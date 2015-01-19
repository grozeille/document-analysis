package grozeille.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import grozeille.DocumentExtractor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by Mathias on 18/01/2015.
 */
public class DocumentExtractorBolt extends BaseBasicBolt {

    private transient DocumentExtractor extractor;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        extractor = new DocumentExtractor();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("data"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            Map<String, Object> data = extractor.extract(input.getString(0), new ByteArrayInputStream(input.getBinary(1)));
            collector.emit(new Values(data));
        } catch (IOException e) {
            collector.reportError(e);
        }
    }
}
