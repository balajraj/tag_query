package com.booking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object TagQueryBatch {


  def groupSteps (path:String, context:SparkContext ): RDD[(String,Iterable[TagStep])] = {
    val fileInput = context.textFile(path)
    val tokens = fileInput.map(line => line.split(",")).filter(_.size == 4). map( y => new TagStep(y(0),y(1),y(2),y(3)) )
    val result:RDD[(String,Iterable[TagStep])] = tokens.keyBy( _.session_id).groupByKey
    result
  }

  def mostRecentTags( result:RDD[(String,Iterable[TagStep])], numTags:Int): Array[(String,(Long,List[TagStep]))] = {
    println("Entering mostRecentTags")

    val values:RDD[(String,(Long,List[TagStep]))] = result.mapValues(iter => (iter.toList(0).getTimeLong,iter.toList)).sortBy( _._1,false)
    val limited = values.take(numTags)
    limited
  }

  def getStepsList( result:RDD[(String,Iterable[TagStep])], stepSize:Int  ) : RDD[List[TagStep]] = {

    val stepOfSize = result.filter(_._2.toList.size == stepSize ).map( x => x._2.toList)
    stepOfSize
  }


}