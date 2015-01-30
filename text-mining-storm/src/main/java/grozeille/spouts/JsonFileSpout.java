package grozeille.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Mathias on 18/01/2015.
 */
public class JsonFileSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(JsonFileSpout.class);

    public static final String INPUT = "jsonfilespout.input";

    private transient String rootPath;
    private transient SpoutOutputCollector spoutOutputCollector;
    private List<String> files = new ArrayList<>();
    private int cptFile = 0;
    private int cptLine = 0;
    private transient BufferedReader reader;
    private transient InputStream is;
    private transient String currentFile;

    private boolean finished = false;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(new String[]{ "json"}));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        rootPath = (String)map.get(INPUT);
        this.spoutOutputCollector = spoutOutputCollector;

        try {
            Files.walkFileTree(new File(rootPath).toPath(), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                    String ext = FilenameUtils.getExtension(file.toString());
                    if (!"json".equalsIgnoreCase(ext)) {
                        LOG.debug("Skip file: " + file);
                        return FileVisitResult.CONTINUE;
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Reading: " + file);
                    }

                    try {


                        files.add(file.toString());

                    } catch (Exception e) {
                        LOG.error("Unable to read " + file, e);
                    } finally {
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOG.error("Unable to list files", e);
        }
    }

    @Override
    public void nextTuple() {
        if(finished){
            return;
        }

        if(reader == null){

            if(cptFile >= files.size()){
                spoutOutputCollector.emit(new Values("flush"));
                finished = true;
                return;
            }

            currentFile = files.get(cptFile);
            try {

                is = new BufferedInputStream(new FileInputStream(currentFile));
                reader = new BufferedReader(new InputStreamReader(is));

            } catch (Exception e) {
                LOG.error("Unable to read " + currentFile, e);
            } finally {

            }

            cptFile++;
            cptLine = 0;
        }

        String json = null;
        try {
            json = reader.readLine();
            cptLine++;
        } catch (IOException e) {
            LOG.error("Unable to read line", e);
        }
        catch(Exception e){
            LOG.error("Unable to read line", e);
        }

        if(json == null){

            try {
                is.close();
            } catch (IOException e) {
                LOG.error("Unable to close file", e);
            }
            reader = null;
        }
        else {
            spoutOutputCollector.emit(new Values(json), currentFile+"_"+cptLine);
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        LOG.debug("ack " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        LOG.error("fail "+msgId);
    }
}
