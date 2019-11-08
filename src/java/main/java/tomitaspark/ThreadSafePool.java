package tomitaspark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Многопоточная обертка для объектов, реализующих интерфейс {@link ProcessorInterface}.
 * <p>
 * Использует для обработки пул объектов {@link ProcessorInterface}, одинаковым образом сконфигурированных.
 * Для обработки будет использован первый свободный обработчик. Поскольку кол-во создаваемых
 * обработчиков равно кол-ву потоков в пуле, то всегда будет как минимум  один доступный обработчик для задачи.
 * Реализация базирована на коде com.srg.tomita.TomitaPooledParser
 */
public class ThreadSafePool<I, O, T extends ProcessorInterface<I, O>> implements ProcessorInterface<I, Future<O>> {

    private static final String PROCESSOR_THREAD_PREFIX = "PROCESSOR-THREAD";

    private static final int TERMINATION_TIMEOUT_SEC = 80;
    private static final int PROCESSOR_WAIT_TIMEOUT_SEC = 120;

    private final int nThreads;

    private final List<T> processors;
    /*
     * Чтобы не зависеть от реализации тредпула, не полагаемся на thread local потоков внутри пула и
     * содержим свою очередь парсеров.
     */
    private final BlockingQueue<T> processorQueue;

    private final ExecutorService executor;

    private static ThreadFactory PROCESSOR_THREAD_FACTORY = new ThreadFactory() {
        private final AtomicInteger number = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, PROCESSOR_THREAD_PREFIX + "-" + number.getAndIncrement());
        }
    };

    public ThreadSafePool(ProcessorFactory<T> factory, int nThreads) throws Exception {
        if (nThreads <= 0) {
            throw new IllegalArgumentException("The positive number of threads is expected");
        }

        this.nThreads = nThreads;

        this.processors = new ArrayList<>(nThreads);
        this.processorQueue = new LinkedBlockingQueue<>(nThreads);

        for (int i = 0; i < nThreads; i++) {
            T parser = factory.get();
            this.processors.add(parser);
            this.processorQueue.add(parser);
        }

        executor = Executors.newFixedThreadPool(nThreads, PROCESSOR_THREAD_FACTORY);
    }

    public int getThreadCount() {
        return nThreads;
    }

    /**
     * Метод выполнит асинхронный парсинг предоставленных документов в первом свободном парсере.
     *
     * @param input входные документы, которые будут парсится
     * @return Future с результатами парсинга.
     */
    public Future<O> parse(I input) {
        return executor.submit(() -> tryParse(input));
    }

    private O tryParse(I input) throws InterruptedException, Exception {
        // Хотя при количестве парсеров == количеству потоков в тредпуле никогда не может
        // случиться ситуации, что нет свободных парсеров (так как каждый поток возвращает парсер
        // в очередь до своего завершения, желательно вызывать блокирующий метод с таймаутом
        T parser = processorQueue.poll(PROCESSOR_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (parser == null) { // Мы не получили свободный парсер в течении таймаута
            throw new IllegalStateException("Failed to obtain free replacer");
        }

        O output;
        try {
            output = parser.parse(input);
        } finally {
            processorQueue.offer(parser);
        }

        return output;
    }

    public synchronized void dispose() throws InterruptedException, Exception {
        this.executor.shutdown();
        if (!this.executor.awaitTermination(TERMINATION_TIMEOUT_SEC, TimeUnit.SECONDS)) {
            throw new IllegalStateException(String.format(
                    "Failed to shutdown thread pool in %d sec",
                    TERMINATION_TIMEOUT_SEC));
        }

        for (T parser : this.processors) {
            parser.dispose();
        }
    }
}

