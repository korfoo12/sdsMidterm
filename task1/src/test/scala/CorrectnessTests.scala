import org.scalatest.FunSuite
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.scalatest._

class CorrectnessTests extends FunSuite with BeforeAndAfterAll {
    var spark : org.apache.spark.sql.SparkSession = _
    override def beforeAll = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    }   

    //test 1
    test("Counting Number of Vertices") {
        val graph = spark.sparkContext.parallelize(List((1,2),(2,3),(3,1)))
        assert(FourHops.countVertices(graph) == 3)
    }

    //test 2
    test("Correctness of mmMapper") {
        val ans = FourHops.mmMapper(3, (1, 2))
        val truth = List(((1,0),(1,2)), ((0,2),(0,1)), ((1,1),(1,2)), ((1,2),(0,1)), ((1,2),(1,2)), ((2,2),(0,1)))
        assert(truth.toSet.equals(ans.toSet))
    }
    
    //test 3
    test("Correctness of mmReducer: value > 0") {
        val indexes = (1,3) 
        val iterables = List((1,2), (0,2))
        val truth = (1, 3, 1)
        assert(FourHops.mmReducer(indexes, iterables).equals(truth))
    }

    //test 4
    test("Correctness of mmReducer: value = 0") {
        val indexes = (2,0) 
        val iterables = List((1,0), (0,2))
        val truth = (2, 0, 0)
        assert(FourHops.mmReducer(indexes, iterables).equals(truth))
    }

    //test 5
    test("Correct 4-hop neighbors for a ring of size 4") {
        val datafile = "src/test/resources/cycledirected.csv"
        val graph = FourHops.loadData(spark, datafile)
        val ans = FourHops.matrixMultiply(FourHops.matrixMultiply(graph, 4), 4).collect().toSet
        val truth = Set((0,0),(1,1),(2,2),(3,3))
        assert(truth.equals(ans))
    }
}