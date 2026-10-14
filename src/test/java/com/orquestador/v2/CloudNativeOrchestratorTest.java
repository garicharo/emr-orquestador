package com.orquestador.v2;

import com.orquestador.Task;
import com.orquestador.WriteMode;
import com.orquestador.v1.BatchMessage;
import com.orquestador.v1.Notifier;
import com.orquestador.v1.TaskCatalog;
import com.orquestador.v1.TaskQueue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloudNativeOrchestratorTest {

    @Test
    void remoteJobsRunIndependentTablesTogetherAndStillWaitForAParent() throws Exception {
        Memory memory = new Memory();
        CountDownLatch bothRoots = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Set<String> started = ConcurrentHashMap.newKeySet();
        AtomicBoolean childSawParent = new AtomicBoolean();
        ServerlessJobs jobs = task -> {
            started.add(task.id());
            if ("T7".equals(task.id())) {
                childSawParent.set("SUCCESS".equals(memory.status.get("b/T6")));
                return CompletableFuture.completedFuture(null);
            }
            bothRoots.countDown();
            return CompletableFuture.runAsync(() -> await(release));
        };
        Thread run = new Thread(() -> new CloudNativeOrchestrator(
                RunTarget.GLUE_JOB, jobs, memory, memory, memory).run(batch()));
        run.start();
        assertTrue(bothRoots.await(5, TimeUnit.SECONDS));
        assertFalse(started.contains("T7"));
        release.countDown();
        run.join(5000);
        assertTrue(childSawParent.get());
        assertEquals(Set.of("b-receipt"), memory.deleted);
    }

    @Test
    void aFailedRemoteJobLeavesTheMessageOnTheQueue() {
        Memory memory = new Memory();
        ServerlessJobs jobs = task -> {
            if ("T6".equals(task.id())) {
                return CompletableFuture.failedFuture(new IllegalStateException("glue job failed"));
            }
            return CompletableFuture.completedFuture(null);
        };
        new CloudNativeOrchestrator(RunTarget.EMR_SERVERLESS, jobs, memory, memory, memory).run(batch());
        assertEquals("FAILED", memory.status.get("b/T6"));
        assertEquals("SUCCESS", memory.status.get("b/T10"));
        assertFalse(memory.status.containsKey("b/T7"));
        assertTrue(memory.deleted.isEmpty());
        assertEquals(List.of("BATCH_FAILED"), memory.notices);
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

    private static BatchMessage batch() {
        return new BatchMessage("b", "b-receipt", List.of(
                new Task("T7", "T6", WriteMode.HISTORICO, "analytics", "hijo"),
                new Task("T6", null, WriteMode.DELTA, "analytics", "padre"),
                new Task("T10", null, WriteMode.PARTITION, "analytics", "independiente")
        ));
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
