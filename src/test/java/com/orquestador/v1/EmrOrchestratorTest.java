package com.orquestador.v1;

import com.orquestador.Task;
import com.orquestador.WriteMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmrOrchestratorTest {

    @Test
    void independentTasksOverlapAndAChildWaitsEvenWhenItArrivesFirst() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Memory memory = new Memory();
        CountDownLatch bothRootsStarted = new CountDownLatch(2);
        CountDownLatch releaseRoots = new CountDownLatch(1);
        Set<String> started = ConcurrentHashMap.newKeySet();
        AtomicBoolean childSawParent = new AtomicBoolean();
        TableWriter writer = task -> {
            started.add(task.id());
            if ("T7".equals(task.id())) {
                childSawParent.set("SUCCESS".equals(memory.status.get("b/T6")));
                return;
            }
            bothRootsStarted.countDown();
            await(releaseRoots);
        };
        Thread run = new Thread(() -> engine(pool, writer, memory).run(batch(), 0));
        run.start();
        assertTrue(bothRootsStarted.await(5, TimeUnit.SECONDS));
        assertFalse(started.contains("T7"));
        releaseRoots.countDown();
        run.join(5000);
        assertFalse(run.isAlive());
        assertTrue(childSawParent.get());
        assertEquals(Set.of("b-receipt"), memory.deleted);
        assertEquals(List.of("BATCH_SUCCESS"), memory.notices);
        pool.shutdown();
    }

    @Test
    void aFailedParentSkipsItsChildAndKeepsTheQueueMessage() {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        Memory memory = new Memory();
        Set<String> wrote = ConcurrentHashMap.newKeySet();
        TableWriter writer = task -> {
            wrote.add(task.id());
            if ("T6".equals(task.id())) {
                throw new IllegalStateException("spark write failed");
            }
        };
        engine(pool, writer, memory).run(batch(), 0);
        assertTrue(wrote.contains("T6"));
        assertTrue(wrote.contains("T10"));
        assertFalse(wrote.contains("T7"));
        assertEquals("FAILED", memory.status.get("b/T6"));
        assertEquals("SUCCESS", memory.status.get("b/T10"));
        assertFalse(memory.status.containsKey("b/T7"));
        assertTrue(memory.deleted.isEmpty());
        assertEquals(List.of("BATCH_FAILED"), memory.notices);
        pool.shutdown();
    }

    @Test
    void anHourLongWriteHoldsTheCluster() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Memory memory = new Memory();
        IdleWatch idle = new IdleWatch(Duration.ofMinutes(5), 0);
        CountDownLatch insideWrite = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        TableWriter writer = task -> {
            insideWrite.countDown();
            await(releaseWrite);
        };
        EmrOrchestrator orchestrator = new EmrOrchestrator(pool, writer, memory, memory, memory, idle);
        Thread run = new Thread(() -> orchestrator.run(
                new BatchMessage("b", "b-receipt", List.of(heavy())),
                Duration.ofHours(1).toNanos()));
        run.start();
        assertTrue(insideWrite.await(5, TimeUnit.SECONDS));
        assertEquals(1, idle.active());
        assertFalse(idle.shouldStop(Duration.ofHours(1).toNanos()));
        releaseWrite.countDown();
        run.join(5000);
        assertEquals(0, idle.active());
        pool.shutdown();
    }

    @Test
    void aCycleKeepsTheMessage() {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Memory memory = new Memory();
        engine(pool, task -> { }, memory).run(new BatchMessage("b", "b-receipt", List.of(
                new Task("T8", "T9", WriteMode.DELTA, "analytics", "a"),
                new Task("T9", "T8", WriteMode.DELTA, "analytics", "b")
        )), 0);
        assertTrue(memory.deleted.isEmpty());
        assertEquals(List.of("BATCH_FAILED"), memory.notices);
        pool.shutdown();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }

    private static EmrOrchestrator engine(ExecutorService pool, TableWriter writer, Memory memory) {
        return new EmrOrchestrator(pool, writer, memory, memory, memory, new IdleWatch(Duration.ofMinutes(5), 0));
    }

    private static BatchMessage batch() {
        return new BatchMessage("b", "b-receipt", List.of(
                new Task("T7", "T6", WriteMode.HISTORICO, "analytics", "hijo"),
                new Task("T6", null, WriteMode.DELTA, "analytics", "padre"),
                new Task("T10", null, WriteMode.PARTITION, "analytics", "independiente")
        ));
    }

    private static Task heavy() {
        return new Task("T10", null, WriteMode.DELTA, "analytics", "pesada");
    }

    private static final class Memory implements TaskCatalog, Notifier, TaskQueue {
        final ConcurrentHashMap<String, String> status = new ConcurrentHashMap<>();
        final List<String> notices = Collections.synchronizedList(new ArrayList<>());
        final Set<String> deleted = ConcurrentHashMap.newKeySet();

        @Override
        public void update(String batchId, String taskId, String status) {
            this.status.put(batchId + "/" + taskId, status);
        }

        @Override
        public void publish(String batchId, String outcome) {
            notices.add(outcome);
        }

        @Override
        public void delete(String receiptHandle) {
            deleted.add(receiptHandle);
        }
    }
}
