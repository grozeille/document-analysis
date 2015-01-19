package grozeille.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Iterables;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Mathias on 18/01/2015.
 */
public class LocalFileSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSpout.class);

    public static final String INPUT = "localfilespout.input";
    public static final String EXTENSIONS = "localfilespout.extensions";

    private transient String rootPath;
    private transient List<String> extensions;
    private transient SpoutOutputCollector spoutOutputCollector;
    private List<String> files = new ArrayList<>();
    private int cpt = 0;

    private boolean finished = false;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(new String[]{ "path", "body"}));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        rootPath = (String)map.get(INPUT);
        extensions = Arrays.asList(((String) map.get(EXTENSIONS)).split(","));
        this.spoutOutputCollector = spoutOutputCollector;

        try {
            Files.walkFileTree(new File(rootPath).toPath(), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                    if (extensions != null && extensions.size() > 0) {
                        String ext = FilenameUtils.getExtension(file.toString());
                        if (!Iterables.contains(extensions, ext)) {
                            LOG.debug("Skip file: " + file);
                            return FileVisitResult.CONTINUE;
                        }
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

        if(cpt >= files.size()){
            finished = true;
            return;
        }

        String file = files.get(cpt);
        cpt++;

        InputStream is = null;
        try {

            is = new BufferedInputStream(new FileInputStream(file));
            byte[] bytes = IOUtils.toByteArray(is);
            spoutOutputCollector.emit(new Values(file, bytes), file);


        } catch (Exception e) {
            LOG.error("Unable to read " + file, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    LOG.error("Unable to close file "+ file, e);
                }
            }
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
