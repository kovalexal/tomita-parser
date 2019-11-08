package tomitaspark;

public interface ProcessorFactory<T extends ProcessorInterface> {
    public T get() throws Exception;
}
