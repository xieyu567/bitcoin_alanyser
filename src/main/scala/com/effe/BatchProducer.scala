package com.effe

import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Column, Dataset, SparkSession}

object BatchProducer {
    def jsonToHttpTransactions(json: String)(implicit spark: SparkSession): Dataset[HttpTransaction] = {
        import spark.implicits._

        val ds: Dataset[String] = Seq(json).toDS()
        val txSchema: StructType = Seq.empty[HttpTransaction].toDS().schema
        val schema: ArrayType = ArrayType(txSchema)
        val arrayColumn: Column = from_json($"value", schema)
        ds.select(explode(arrayColumn).alias("v"))
            .select("v.*")
            .as[HttpTransaction]
    }
}
