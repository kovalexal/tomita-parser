package tomitaspark;

public class ProcessorPipeline<I, O> implements ProcessorInterface<I, O> {
    private ProcessorInterface[] pipelineProcessors;
    private boolean debugPrint = false;

    public ProcessorPipeline(boolean debug, ProcessorFactory... processors) throws Exception {
        this(processors);
        debugPrint = debug;
    }

    public ProcessorPipeline(ProcessorFactory... processors) throws Exception {
        pipelineProcessors = new ProcessorInterface[processors.length];

        for (int i = 0; i < processors.length; ++i)
            pipelineProcessors[i] = processors[i].get();
    }

    public O parse(I input) throws Exception {
        Object result = input;

        if (debugPrint) {
            System.out.println();
            System.out.println(result);
        }

        for (ProcessorInterface processor: pipelineProcessors) {
            result = processor.parse(result);
            if (debugPrint)
                System.out.println(result);
        }

        if (debugPrint)
            System.out.println();

        return (O) result;
    }

    public void dispose() throws Exception {
        for (ProcessorInterface processor: pipelineProcessors)
            processor.dispose();
    }
}
