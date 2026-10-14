package com.orquestador.v1;

import com.orquestador.Task;
import com.orquestador.TaskGraph;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Version 1, the system that ran on EMR.
 * One pool runs both kinds of work: a task with a parent waits without holding a thread,
 * and a task with no parent starts immediately. Arrival order does not matter.
 * A short write and an hour-long write use the same path. Spark stays inside {@link TableWriter}.
 */
public final class EmrOrchestrator {

    private final Executor emrDriverPool;
    private final TableWriter writer;
    private final TaskCatalog catalog;
    private final Notifier notifier;
    private final TaskQueue queue;
    private final IdleWatch idle;

    public EmrOrchestrator(
            Executor emrDriverPool,
            TableWriter writer,
            TaskCatalog catalog,
            Notifier notifier,
            TaskQueue queue,
            IdleWatch idle) {
        this.emrDriverPool = emrDriverPool;
        this.writer = writer;
        this.catalog = catalog;
        this.notifier = notifier;
        this.queue = queue;
        this.idle = idle;
    }

    public void run(BatchMessage message, long finishedAtNanos) {
        idle.started();
        try {
            try {
                settle(message);
                queue.delete(message.receiptHandle());
                notifier.publish(message.batchId(), "BATCH_SUCCESS");
            } catch (RuntimeException exception) {
                notifier.publish(message.batchId(), "BATCH_FAILED");
            }
        } finally {
            idle.finished(finishedAtNanos);
        }
    }

    private void settle(BatchMessage message) {
        Map<String, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        for (Task task : TaskGraph.order(message.tasks())) {
            CompletableFuture<Void> future = task.dependsOn() == null
                    ? CompletableFuture.runAsync(() -> execute(message.batchId(), task), emrDriverPool)
                    : futures.get(task.dependsOn()).thenRunAsync(() -> execute(message.batchId(), task), emrDriverPool);
            futures.put(task.id(), future);
        }
        CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).join();
    }

    private void execute(String batchId, Task task) {
        catalog.update(batchId, task.id(), "RUNNING");
        try {
            writer.write(task);
            catalog.update(batchId, task.id(), "SUCCESS");
        } catch (RuntimeException exception) {
            catalog.update(batchId, task.id(), "FAILED");
            throw exception;
        }
    }
}
