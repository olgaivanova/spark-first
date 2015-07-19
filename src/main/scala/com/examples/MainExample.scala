package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.util.StatCounter

/** class NAStatCounter extends Serializable{
  
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0
  
  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)){
      missing +=1
    } else {
      stats.merge(x)
    }
    this 
  }
    
def merge(other: NAStatCounter): NAStatCounter = {
  stats.merge(other.stats)
  missing += other.missing
  this
}

override def toString = {
  "stats: " + stats.toString +" NaN: " + missing
  }
} 

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}
*/
object MainExample {
  
  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }
  
  def toDouble(s: String) = {
    if ("?".equals(s)) Double.NaN else s.toDouble
  }
  
  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)
  
  def parse(line: String) = {
    val pieces = line.split(',')
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }
  
  def main(arg: Array[String]) {
    
   
    
   /** val conf = new SparkConf().setAppName("MainExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val rawblocks = sc.textFile("linkage/donation")
    
    val head = rawblocks.take(10)
   
    head.filter(isHeader).foreach(println)
    
    val line = head(5)
    val md = parse(line)
    
    println(md.matched)
    
    val noheader = rawblocks.filter(x => !isHeader(x))
    val parsed = noheader.map(line => parse(line))
    val matchCounts = parsed.map(md => md.matched).countByValue()
    println(matchCounts)
    println("recompiled")
    val matchCountsSeq = matchCounts.toSeq
    matchCountsSeq.sortBy(_._1).foreach(println)
    println("sortby2")
    matchCountsSeq.sortBy(_._2).foreach(println)
    println("reversed")
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)
    println("stats")
    
    import java.lang.Double.isNaN
    
    val StatCount = parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()
    println(StatCount)
   
    val stats = (0 until 9).map(i => {
      parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
    })
    val stat1 = stats(1)
    println(stat1)
  
    val countstat8 = stats(8)
    println("stat8working")
    println(countstat8)
    **/
    
     
    println("helloNewMain")
    val nastats = NAStatCounter.apply(17.29)
    print(nastats)
    
    val nas1 = NAStatCounter(10.0)
    println(nas1)
    nas1.add(2.1)
    println("nas1 + 2" + nas1)
    val nas2 = NAStatCounter(Double.NaN)
    println("merge = " + nas1.merge(nas2))
    
    val arr = Array(1.0, Double.NaN, 17.29)
    val nas = arr.map(d => NAStatCounter(d))
    println(nas)
    
    
  }
}
