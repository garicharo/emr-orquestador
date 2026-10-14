package com.orquestador.v1;

import com.orquestador.Task;

/**
 * On the cluster this call was Scala Spark: delta, histórico, or partitioned write,
 * published through the Glue Data Catalog. Here it is the port the Java pool invokes.
 */
public interface TableWriter {
    void write(Task task);
}
