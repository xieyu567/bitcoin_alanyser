package com.effe

import cats.effect.{ExitCode, IO, IOApp}
import com.effe.BatchProducer.{httpToDomainTransactions, jsonToHttpTransactions}
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{Dataset, SparkSession}

import java.net.{URI, URL}
import scala.io.Source

class BatchProducerApp extends IOApp with StrictLogging {
    implicit val spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate()
    implicit val appContext: AppContext = new AppContext(new URI("./data/transactions"))

    def bitstampUrl(timeParam: String): URL =
        new URL("https://www.bitstamp.net/api/v2/transactions/btcusd?time=" + timeParam)

    def transactionIO(timeParam: String): IO[Dataset[Transaction]] = {
        val url = bitstampUrl(timeParam)
        val jsonIO = IO {
            logger.info(s"calling $url")
            Source.fromURL(url).mkString
        }
        jsonIO.map(json =>
        httpToDomainTransactions(jsonToHttpTransactions(json)))
    }

    val initialJsonTxs: IO[Dataset[Transaction]] = transactionIO("day")
    val nextJsonTxs: IO[Dataset[Transaction]] = transactionIO("hour")

    def run(args: List[String]): IO[ExitCode] =
        BatchProducer.processRepeatedly(initialJsonTxs, nextJsonTxs).map(_ => ExitCode.Success)
}

object BatchProducerAppSpark extends BatchProducerApp
