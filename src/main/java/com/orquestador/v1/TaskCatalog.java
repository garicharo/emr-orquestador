package com.orquestador.v1;

/** DynamoDB row for one task: batch id, task id, and status. */
public interface TaskCatalog {
    void update(String batchId, String taskId, String status);
}
