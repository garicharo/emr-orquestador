package com.orquestador.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 * Shape of the write that ran on the EMR cluster, with the session pointed at
 * the Glue Data Catalog. This file is not compiled by Maven and it is not a
 * recovered production job.
 */
object GlueTableWrite {

  def write(
      spark: SparkSession,
      mode: String,
      df: DataFrame,
      database: String,
      table: String,
      path: String,
      partitionColumn: String
  ): Unit = {
    val writing = df.write.mode(if (mode == "HISTORICO") "append" else "overwrite")
    mode match {
      case "HISTORICO" =>
        writing.format("parquet").option("path", path).saveAsTable(s"$database.$table")
      case "PARTITION" =>
        writing
          .format("parquet")
          .partitionBy(partitionColumn)
          .option("path", path)
          .saveAsTable(s"$database.$table")
      case "DELTA" =>
        writing.format("delta").option("path", path).saveAsTable(s"$database.$table")
      case other =>
        throw new IllegalArgumentException(other)
    }
  }
}
