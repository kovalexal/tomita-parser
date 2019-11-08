package tomitaspark;

public interface ProcessorInterface<I, O> {
    O parse(I input) throws Exception;
    void dispose() throws Exception;
}
