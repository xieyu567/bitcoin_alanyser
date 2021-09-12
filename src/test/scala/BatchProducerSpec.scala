import com.effe.{BatchProducer, HttpTransaction, Transaction}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class BatchProducerSpec extends SharedSparkSession with Matchers {
    val httpTransaction1: HttpTransaction = HttpTransaction("1532365695", "70683282", "7740.00", "0", "0.10041719")
    val httpTransaction2: HttpTransaction = HttpTransaction("1532365693", "70683281", "7739.99", "0", "0.00148564")


    test("create a Dataset[HttpTransation] from a Json string") {
        val json =
            """
              |[{"date": "1532365695", "tid": "70683282", "price": "7740.00", "type": "0", "amount": "0.10041719"},
              |{"date": "1532365693", "tid": "70683281", "price": "7739.99", "type": "0", "amount": "0.00148564"}]
              |""".stripMargin

        val ds: Dataset[HttpTransaction] = BatchProducer.jsonToHttpTransactions(json)
        ds.collect() should contain theSameElementsAs Seq(httpTransaction1, httpTransaction2)
    }


    test("transform a Dataset[HttpTransaction] into a Dataset[Transaction]") {
        import testImplicits._
        val source: Dataset[HttpTransaction] = Seq(httpTransaction1, httpTransaction2).toDS()
        val target = BatchProducer.httpToDomainTransactions(source)
        val transaction1: Transaction = Transaction(
            timestamp = new Timestamp(1532365695000L), tid = 70683282,
            price = 7740.00, sell = false, amount = 0.10041719)
        val transaction2: Transaction = Transaction(
            timestamp = new Timestamp(1532365693000L), tid = 70683281,
            price = 7739.99, sell = false, amount = 0.00148564)

        target.collect() should contain theSameElementsAs Seq(transaction1, transaction2)
    }
}

