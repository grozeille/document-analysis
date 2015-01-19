package grozeille.loaders;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import grozeille.FileItem;
import grozeille.framework.Loader;
import grozeille.workers.DocumentExtractorWorker;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Mathias on 18/01/2015.
 */
public class FileLoader extends Loader<FileItem> {

    private static final Logger LOG = LoggerFactory.getLogger(FileLoader.class);

    private final String rootPath;
    private final List<String> extensions;

    public FileLoader(int taskId, MetricRegistry registry, List<BlockingQueue<FileItem>> outputs, String rootPath, List<String> extensions) {
        super(taskId, registry, outputs);
        this.rootPath = rootPath;
        this.extensions = extensions;
    }

    @Override
    protected FileItem readItem() throws Exception {
        try {
            Files.walkFileTree(new File(rootPath).toPath(), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                    if(extensions != null && extensions.size() > 0){
                        String ext = FilenameUtils.getExtension(file.toString());
                        if(!Iterables.contains(extensions, ext)){
                            LOG.debug("Skip file: " + file);
                            return FileVisitResult.CONTINUE;
                        }
                    }

                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Reading: " + file);
                    }

                    InputStream is = null;
                    try {

                        is = new BufferedInputStream(new FileInputStream(file.toFile()));
                        byte[] bytes = IOUtils.toByteArray(is);
                        FileItem item = new FileItem();
                        item.setPath(file.toString());
                        item.setBody(bytes);
                        submitItem(item);

                    } catch (Exception e) {
                        LOG.error("Unable to read " + file, e);
                    } finally {
                        is.close();
                        incProcessCount();
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
            LOG.error("Unable to read files", e);
        }

        return null;
    }
}
