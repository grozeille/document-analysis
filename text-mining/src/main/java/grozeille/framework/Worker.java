package grozeille.framework;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Mathias on 18/01/2015.
 */
public abstract class Worker<TInput, TOutput> extends Task<TOutput> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private final BlockingQueue<TInput> input;

    public Worker(int taskId, MetricRegistry registry, BlockingQueue<TInput> input, List<BlockingQueue<TOutput>> outputs){
        super(taskId, registry, outputs);

        this.input = input;
    }

    @Override
    public void run() {
        TInput item = null;

        try {
            init();
        } catch (Exception e) {
            LOG.error("Unable to init", e);
            return;
        }

        try {
            do {

                try {
                    item = input.poll(60, TimeUnit.MINUTES);
                    if(item == null) {
                        break;
                    }

                    TOutput result = processItem(item);
                    if (result != null) {
                        submitItem(result);
                    }

                } catch (Exception e) {
                    LOG.error("Error during processing of item", e);
                    incErrorCount();
                } finally {
                    incProcessCount();
                }
            } while (item != null);
        }finally {
            shutdown();
        }
    }

    protected abstract TOutput processItem(TInput item) throws Exception;

    protected void init() throws Exception {

    }

    protected void shutdown(){

    }
}
