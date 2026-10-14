package com.orquestador.v2;

import com.orquestador.Task;

import java.util.concurrent.CompletableFuture;

/**
 * Version 2 starts the write somewhere else: a Glue job or an EMR Serverless job.
 * The future completes when that remote run finishes. This process does not host Spark.
 */
public interface ServerlessJobs {
    CompletableFuture<Void> start(Task task);
}
