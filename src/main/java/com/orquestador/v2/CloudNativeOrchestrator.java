package com.orquestador.v2;

import com.orquestador.Task;
import com.orquestador.TaskGraph;
import com.orquestador.v1.BatchMessage;
import com.orquestador.v1.Notifier;
import com.orquestador.v1.TaskCatalog;
import com.orquestador.v1.TaskQueue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Version 2, the later shape of the same contract.
 * Glue jobs or EMR Serverless run the Spark write. Java still reads the queue,
 * the DynamoDB row, and sends SNS, and it still respects dependencies.
 * There is no long-lived cluster and no idle clock in this process:
 * the platform stops the compute when the remote job ends.
 */
public final class CloudNativeOrchestrator {

    private final RunTarget target;
    private final ServerlessJobs jobs;
    private final TaskCatalog catalog;
    private final Notifier notifier;
    private final TaskQueue queue;

    public CloudNativeOrchestrator(
            RunTarget target,
            ServerlessJobs jobs,
            TaskCatalog catalog,
            Notifier notifier,
            TaskQueue queue) {
        this.target = target;
        this.jobs = jobs;
        this.catalog = catalog;
        this.notifier = notifier;
        this.queue = queue;
    }

    public RunTarget target() {
        return target;
    }

    public void run(BatchMessage message) {
        try {
            settle(message);
            queue.delete(message.receiptHandle());
            notifier.publish(message.batchId(), "BATCH_SUCCESS");
        } catch (RuntimeException exception) {
            notifier.publish(message.batchId(), "BATCH_FAILED");
        }
    }

    private void settle(BatchMessage message) {
        Map<String, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        for (Task task : TaskGraph.order(message.tasks())) {
            CompletableFuture<Void> future = task.dependsOn() == null
                    ? startRemote(message.batchId(), task)
                    : futures.get(task.dependsOn()).thenCompose(ignored -> startRemote(message.batchId(), task));
            futures.put(task.id(), future);
        }
        CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).join();
    }

    private CompletableFuture<Void> startRemote(String batchId, Task task) {
        catalog.update(batchId, task.id(), "RUNNING");
        return jobs.start(task).whenComplete((ignored, error) -> {
            if (error == null) {
                catalog.update(batchId, task.id(), "SUCCESS");
            } else {
                catalog.update(batchId, task.id(), "FAILED");
            }
        });
    }
}
