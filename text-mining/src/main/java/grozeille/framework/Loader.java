package grozeille.framework;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Mathias on 18/01/2015.
 */
public abstract class Loader<TOutput> extends Task<TOutput> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);

    public Loader(int taskId, MetricRegistry registry, List<BlockingQueue<TOutput>> outputs) {
        super(taskId, registry, outputs);
    }

    @Override
    public void run() {
        while(true){

            TOutput result = null;
            try {
                result = readItem();
                if (result != null) {
                    submitItem(result);
                }
            }catch (Exception e){
                LOG.error("Unable to read item", e);
                incErrorCount();
            } finally {
                if(result != null) {
                    incProcessCount();
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOG.error("Stop sleep", e);
            }
        }
    }

    protected abstract TOutput readItem() throws Exception;
}
