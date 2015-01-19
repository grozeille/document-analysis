package grozeille.workers;

import com.codahale.metrics.MetricRegistry;
import grozeille.DocumentExtractor;
import grozeille.FileItem;
import grozeille.framework.Worker;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Mathias on 11/01/2015.
 */
public class DocumentExtractorWorker extends Worker<FileItem, Map<String, Object>> {

    private final DocumentExtractor extractor = new DocumentExtractor();

    public DocumentExtractorWorker(int workerId, MetricRegistry registry, BlockingQueue<FileItem> input, List<BlockingQueue<Map<String, Object>>> outputs) {
        super(workerId, registry, input, outputs);
    }

    @Override
    protected Map<String, Object> processItem(FileItem item) throws Exception {
        return extractor.extract(item.getPath(), new ByteArrayInputStream(item.getBody()));
    }
}
