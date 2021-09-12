import com.effe.{BatchProducer, Transaction}

import java.sql.Timestamp
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class BatchProducerIT extends SharedSparkSession with Matchers {

    import testImplicits._

    test("save a Dataset[Transaction] to parquet") {
        withTempDir { tmpDir =>
            val transaction1 = Transaction(timestamp = new Timestamp(1532365695000L),
                tid = 70683282, price = 7740.00, sell = false, amount = 0.10041719)
            val transaction2 = Transaction(timestamp = new Timestamp(1532365693000L),
                tid = 70683281, price = 7739.00, sell = false, amount = 0.00148564)
            val sourceDS = Seq(transaction1, transaction2).toDS()

            val uri = tmpDir.toURI
            BatchProducer.unsafeSave(sourceDS, uri)
            tmpDir.list() should contain("date=2018-07-24")
            val readDS = spark.read.parquet(uri.toString).as[Transaction]
            sourceDS.collect() should contain theSameElementsAs readDS.collect()
        }
    }
}

