package com.orquestador.v1;

/** The queue that carries the batch. Delete only after every write in the batch has finished well. */
public interface TaskQueue {
    void delete(String receiptHandle);
}
