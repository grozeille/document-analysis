package grozeille.framework;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import grozeille.workers.DocumentExtractorWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Mathias on 18/01/2015.
 */
public abstract class Task<TOutput> {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    private final List<BlockingQueue<TOutput>> outputs;

    private final int taskId;

    private final MetricRegistry registry;
    private final Meter meterProcess;
    private final Counter counterError;

    public Task(int taskId, MetricRegistry registry, List<BlockingQueue<TOutput>> outputs){

        this.outputs = outputs;

        this.taskId = taskId;
        this.registry = registry;

        this.meterProcess = registry.meter(MetricRegistry.name(DocumentExtractorWorker.class, "process"));
        this.counterError = registry.counter(MetricRegistry.name(DocumentExtractorWorker.class, "error"));
    }

    protected int getTaskId() {
        return taskId;
    }

    protected MetricRegistry getRegistry() {
        return registry;
    }

    protected List<BlockingQueue<TOutput>> getOutputs() {
        return outputs;
    }

    protected void submitItem(TOutput item){
        for(BlockingQueue<TOutput> output : outputs){
            try {
                output.put(item);
            } catch (InterruptedException e) {
                LOG.error("Error during processing of item", e);
            }
        }
    }

    protected void incErrorCount(){
        this.counterError.inc();
    }

    protected void incProcessCount(){
        this.meterProcess.mark();
    }
}
