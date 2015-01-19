package grozeille;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import grozeille.loaders.FileLoader;
import grozeille.workers.DocumentExtractorWorker;
import grozeille.workers.JsonWriterWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by Mathias on 10/01/2015.
 */
public class Cluster {

    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

    private static final BlockingQueue<FileItem> filesToRead = new ArrayBlockingQueue<FileItem>(4);
    private static final BlockingQueue<Map<String, Object>> parsedToWrite = new ArrayBlockingQueue<Map<String, Object>>(10);

    public static void main(String[] args) throws IOException, InterruptedException {

        final MetricRegistry registry = new MetricRegistry();

        final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
                .outputTo(LoggerFactory.getLogger(Cluster.class))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);

        registry.register(MetricRegistry.name(Cluster.class, "filesToRead", "size"), (Gauge<Integer>) () -> filesToRead.size());
        registry.register(MetricRegistry.name(Cluster.class, "parsedToWrite", "size"), (Gauge<Integer>) () -> parsedToWrite.size());

        String rootPath = "Z:\\AMINA\\03 A ranger";
        //String rootPath = "C:\\Users\\Mathias\\Documents\\aranger";
        final String rootOutputPath = "C:\\Users\\Mathias\\Documents\\aranger-output2";

        ExecutorService executorService = Executors.newFixedThreadPool(12);

        executorService.submit(new FileLoader(1, registry, Arrays.asList(filesToRead), rootPath, Arrays.asList("pdf")));

        for(int cpt = 0; cpt < 4; cpt++) {
            executorService.submit(new DocumentExtractorWorker(cpt, registry, filesToRead, Arrays.asList(parsedToWrite)));
        }

        for(int cpt = 0; cpt < 2; cpt++) {
            executorService.submit(new JsonWriterWorker(cpt, registry, parsedToWrite, rootOutputPath));
        }

        LOG.info("Started");

        executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    }
}
