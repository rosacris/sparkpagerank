package com.miyagi

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object PageRank {

	def pagerank(input: RDD[(Int, List[Int])]) = {
		
		def shareRank(outLinks: (Int, (Double, List[Int]))): List[(Int,Double)] = {
		  val rank = outLinks._2._1
			val rankPerLink = rank / outLinks._2._2.length 
			
			// Add a 0 sharing for the actual link and share its actual rank with the rest
			(outLinks._1, 0.0D) :: outLinks._2._2.map( x => (x, rankPerLink))
		}

		def applyDamping(rank: Double): Double = {
		  val damping = 0.85D
		  damping * rank + (1 - damping)
		}
				
		// Prepare working set with default rank of 1.0 for each link
		var ranks: RDD[(Int, (Double, List[Int]))] = input.map(x => (x._1, (1.0D, x._2)))

		// Iterate map/reduce/join
		for(i <- 1 to 10) {
			ranks = ranks.flatMap(shareRank).reduceByKey( (a,b) => a + b).mapValues(applyDamping).join(input)
		}

		ranks.collect()
	}
	
	def main(args:Array[String]) = {
	  val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)
    var nodes = List((2,List(1)),(3,List(1)),(1,List()))
    var rdd = sc.parallelize(nodes)
    val result = pagerank(rdd)
    println(result)
	}
	
	
}