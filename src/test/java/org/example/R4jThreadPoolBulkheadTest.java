package org.example;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.ThreadPoolBulkhead;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadConfig;
import io.github.resilience4j.bulkhead.ThreadPoolBulkheadRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

class R4jThreadPoolBulkheadTest {

    private static final Logger logger = LogManager.getLogger(R4jThreadPoolBulkheadTest.class);

    private static final int MAX_POOL_SIZE = 2;
    private static final int CORE_POOL_SIZE = 1;
    private static final int QUEUE_CAPACITY = 1;

    private Runnable sleeping;

    @BeforeEach
    void setUp() {
        sleeping = () -> sleepFor(Duration.ofSeconds(5));
    }

    @Test
    void createsThreadPool() {
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(MAX_POOL_SIZE)
                .coreThreadPoolSize(CORE_POOL_SIZE)
                .queueCapacity(QUEUE_CAPACITY)
                .build();

        ThreadPoolBulkhead bulkhead = ThreadPoolBulkheadRegistry.of(config).bulkhead("test");

        ThreadPoolBulkhead.Metrics metrics = bulkhead.getMetrics();
        assertEquals(MAX_POOL_SIZE, metrics.getMaximumThreadPoolSize());
        assertEquals(CORE_POOL_SIZE, metrics.getCoreThreadPoolSize());
        assertEquals(QUEUE_CAPACITY, metrics.getQueueCapacity());
    }

    @Test
    void runsOneRequest() {
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(MAX_POOL_SIZE)
                .coreThreadPoolSize(CORE_POOL_SIZE)
                .queueCapacity(QUEUE_CAPACITY)
                .build();
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkheadRegistry.of(config).bulkhead("test");

        bulkhead.executeRunnable(sleeping);

        ThreadPoolBulkhead.Metrics metrics = bulkhead.getMetrics();
        assertEquals(1, metrics.getThreadPoolSize());
        assertEquals(1, metrics.getActiveThreadCount());
        assertEquals(1, metrics.getAvailableThreadCount());
        assertEquals(0, metrics.getQueueDepth());
        assertEquals(1, metrics.getRemainingQueueCapacity());
    }

    @Test
    void runsTwoRequests() {
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(MAX_POOL_SIZE)
                .coreThreadPoolSize(CORE_POOL_SIZE)
                .queueCapacity(QUEUE_CAPACITY)
                .build();
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkheadRegistry.of(config).bulkhead("test");

        bulkhead.executeRunnable(sleeping);
        bulkhead.executeRunnable(sleeping);

        ThreadPoolBulkhead.Metrics metrics = bulkhead.getMetrics();
        assertEquals(1, metrics.getThreadPoolSize());
        assertEquals(1, metrics.getActiveThreadCount());
        assertEquals(1, metrics.getAvailableThreadCount());
        assertEquals(1, metrics.getQueueDepth());
        assertEquals(0, metrics.getRemainingQueueCapacity());
    }

    @Test
    void runsThreeRequest() {
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(MAX_POOL_SIZE)
                .coreThreadPoolSize(CORE_POOL_SIZE)
                .queueCapacity(QUEUE_CAPACITY)
                .build();
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkheadRegistry.of(config).bulkhead("test");

        bulkhead.executeRunnable(sleeping);
        bulkhead.executeRunnable(sleeping);
        bulkhead.executeRunnable(sleeping);

        ThreadPoolBulkhead.Metrics metrics = bulkhead.getMetrics();
        assertEquals(2, metrics.getThreadPoolSize());
        assertEquals(2, metrics.getActiveThreadCount());
        assertEquals(0, metrics.getAvailableThreadCount());
        assertEquals(1, metrics.getQueueDepth());
        assertEquals(0, metrics.getRemainingQueueCapacity());
    }

    @Test
    void runsFourRequests() {
        ThreadPoolBulkheadConfig config = ThreadPoolBulkheadConfig.custom()
                .maxThreadPoolSize(MAX_POOL_SIZE)
                .coreThreadPoolSize(CORE_POOL_SIZE)
                .queueCapacity(QUEUE_CAPACITY)
                .build();
        ThreadPoolBulkhead bulkhead = ThreadPoolBulkheadRegistry.of(config).bulkhead("test");

        bulkhead.executeRunnable(sleeping);
        bulkhead.executeRunnable(sleeping);
        bulkhead.executeRunnable(sleeping);

        assertThrowsExactly(BulkheadFullException.class, () -> bulkhead.executeRunnable(sleeping));
    }

    void sleepFor(Duration duration) {
        try {
            logger.info("Going to sleep in this thread...");
            Thread.sleep(duration);
            logger.info("Finished!");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
