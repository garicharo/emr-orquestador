package com.orquestador.v1;

import com.orquestador.Task;

import java.util.List;
import java.util.Objects;

public record BatchMessage(String batchId, String receiptHandle, List<Task> tasks) {

    public BatchMessage {
        Objects.requireNonNull(batchId, "batchId");
        Objects.requireNonNull(receiptHandle, "receiptHandle");
        tasks = List.copyOf(tasks);
    }
}
