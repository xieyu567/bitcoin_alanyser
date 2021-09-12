package com.effe

import cats.effect.IO
import cats.effect.kernel.Temporal
import org.apache.spark.sql.SparkSession

import java.net.URI

class AppContext(val transactionStorePath: URI)
                (implicit val spark: SparkSession) {

}
