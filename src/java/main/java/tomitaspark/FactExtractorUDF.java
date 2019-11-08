package tomitaspark;

import org.apache.spark.sql.api.java.UDF1;

public class FactExtractorUDF implements UDF1<String, String[]> {
    protected static final long serialVersionUID = 1L;

    protected static class ExecutorSettings {
        static final String numThreadsStr = System.getenv("N_THREADS");
        static final int numThreads = numThreadsStr == null ? 1 : Integer.parseInt(numThreadsStr);
    }

    protected static class Pipeline {
        static ThreadSafePool pool;
        static {
            try {
                pool = new ThreadSafePool(
                        () -> new ProcessorPipeline<String, String>(
                            () -> new RegexpReplacer("rules/replaces.json"),
                            () -> new TomitaParserProcessor("rules/config.proto"),
                            () -> new FactParser()
                        ),
                        ExecutorSettings.numThreads);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException("Cannot instantiate ThreadSafePool of Pipeline");
            }
        }
    }
    protected static ThreadSafePool getPipeline() {
        return Pipeline.pool;
    }

    @Override
    public String[] call(String text) throws Exception {
        ThreadSafePool pipelinePool = getPipeline();
        return (String[]) pipelinePool.parse(text).get();
    }
}
