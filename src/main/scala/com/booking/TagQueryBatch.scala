package com.booking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object TagQueryBatch {

  val tagSize = 4

  def groupSteps (path:String, context:SparkContext ): RDD[(String,Iterable[TagStep])] = {
    val fileInput = context.textFile(path)
    val tokens = fileInput.map(line => line.split(",")).filter(_.size == tagSize). map(
      y => new TagStep(y(0),y(1),y(2),y(3)) )
    val result:RDD[(String,Iterable[TagStep])] = tokens.keyBy( _.session_id.trim).groupByKey
    result
  }

  def mostRecentTags( result:RDD[(String,Iterable[TagStep])], numTags:Int): Array[List[TagStep]] = {

    val values:RDD[(String,(Long,List[TagStep]))] = result.mapValues(iter =>
      (iter.toList(0).getTimeLong,iter.toList)).sortBy( _._1,false)
    val limited = values.take(numTags).map(x => x._2._2)
    limited
  }

  def getStepsList( result:RDD[(String,Iterable[TagStep])], stepSize:Int  ) : Array[List[TagStep]] = {
    val filtered:RDD[Iterable[TagStep]] = result.map(_._2.filter( _.isStep) )
    val resultFil = filtered.map( x => x.toList).filter(_.size == stepSize)
    resultFil.collect
  }


}