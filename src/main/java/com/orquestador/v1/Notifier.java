package com.orquestador.v1;

/** SNS notice for a finished batch. */
public interface Notifier {
    void publish(String batchId, String outcome);
}
