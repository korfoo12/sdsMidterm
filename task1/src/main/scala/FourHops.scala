//------------------------------------------------------------------------------
// Ivan Korovkin
//------------------------------------------------------------------------------

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import scala.tools.nsc.io.File

object FourHops {
//------------------------------------------------------------------------------
    def main(args: Array[String]) = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        val datafile: String = "src/test/resources/cycledirected.csv"
        val graph = loadData(spark, datafile)

        // Count total vertices in the graph
        val totalVertices = countVertices(graph)
        println("Total Vertices", totalVertices)


        // Find two hop neighbors
        val two_hop = matrixMultiply(graph, totalVertices)
        
        // Find four hop neighbors
        val quads = matrixMultiply(two_hop, totalVertices)
        
        quads.collect().foreach(println)

        spark.stop()
    }
//------------------------------------------------------------------------------

    type GRAPH = RDD[(Int, Int)]
    
//------------------------------------------------------------------------------
    /*
    This function counts the number of distinct nodes in the input graph. Since each node 
    has atleast degree 1, it should appear in some pair (v1, v2).
    */
    def countVertices(graph: GRAPH): Int =  {
        //checking degenerate case
        if (graph.isEmpty)
            0
        else {
            //assigning all values from RDD pair
            val values = graph.map(x => x._1) ++ graph.map(x => x._2)
            //getting distinct values count out of them
            values.distinct().count().toInt
        }
    }
//------------------------------------------------------------------------------
//  MapReduce
//------------------------------------------------------------------------------
    /*
    This function should output tuples of the form
    ((i, j), (identifying_matrix, identifying_index))
    
    (i, j) refers to the position in output matrix.
    
    Identifying matrix refers to A and B in the matrix product A.B, use 1 as identifier 
    for A and 0 for B.
    
    Identifying index helps multiply correct elements of A and B.
    */

    def mmMapper(N: Int, index: (Int, Int)): List[((Int, Int), (Int, Int))] = {
        //emitting elements of left matrix
        val A = for (k <- 0 until N)
            yield ((index._1, k), (1, index._2))
        //emitting elements of right matrix
        val B = for (k <- 0 until N)
            yield ((k, index._2), (0, index._1))
        //returning list with values
        A.toList ++ B.toList
}

//------------------------------------------------------------------------------

    /*
    The reducer reduces row i of Matrix A and column j of matrix B. Note that it only receives 
    non zero elements on the row/column. The identifying matrix and identifying index in the 
    specified row/column is present in the Iterable[(Int, Int)].

    It must now produce the matrix product value (i, j, value). Set the value to 1 if it is 
    greater than 0 else let it be 0. Refer to test3 and test4 for an example case.
    */
    def mmReducer(productElements: ((Int, Int), Iterable[(Int, Int)])): (Int, Int, Int) = {
        //get indexes of matrix nodes
        val idx = productElements._1
        //group elements by matrix identifier
        val matrix_id_grouped = productElements._2.groupBy(_._1)

        //getting two vectors of elements separated by matrix id
        var Avec: Set[Int] = Set()
        var Bvec: Set[Int] = Set()

        if (matrix_id_grouped.contains(1))
            Avec = matrix_id_grouped(1).map(_._2).toSet

        if (matrix_id_grouped.contains(0))
            Bvec = matrix_id_grouped(0).map(_._2).toSet

        //get indicator of path existence
        val M = Avec.intersect(Bvec).size

        (idx._1, idx._2, M)
    }
//------------------------------------------------------------------------------    
    
    /*
    This function multiplies the adjacency matrix to itself: A.B = A.A = A^2 
    
    The input consists of only pairs which correspond to a directed edge. Hence, if there is no 
    edge between v1 and v2, the pair (v1, v2) will not appear in the input.
    
    Similar to the input, the output must consist only of pairs which correspond to a positive 
    value after multiplication. Hence if, A^2[v1, v2] = 0, the pair (v1, v2) should not appear 
    in the output. 

    Note that this is sufficient for our goal of identifying all 4-hop neighbors of a node. 
    */
    def matrixMultiply(matrix: GRAPH, N: Int): GRAPH = {
        matrix.flatMap(x => mmMapper(N, x))//map values
          .groupByKey()//group by keys to single sequence
          .map(x => mmReducer(x))//reducing values
          .filter(_._3 != 0)//filtering out nodes who has 0 in indicator
          .map(x => (x._1, x._2))//getting final result
    }



//------------------------------------------------------------------------------
//  Auxiliary functions
//------------------------------------------------------------------------------

    def loadData(spark: SparkSession, datafile: String): GRAPH = {
        import spark.implicits._
        
        val graph = spark.read.options(Map("header"->"false"))
            .csv(datafile)
            .rdd
            .map(r => (r.getString(0).toInt, r.getString(1).toInt))
        
        graph
    }

//------------------------------------------------------------------------------
}
