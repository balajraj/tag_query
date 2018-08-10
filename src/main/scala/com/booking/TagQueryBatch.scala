package com.booking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object TagQueryBatch {

  val tagSize = 4

  /**
    * groupSteps will group the records based on sessionId
    * @param path The path to input files can be a directory in hdfs or s3 or local file system
    * @param context the spark context to run the job
    * @return rdd tuple which was grouped the records based on session id
    */
  def groupSteps (path:String, context:SparkContext ): RDD[(String,Iterable[TagStep])] = {
    val fileInput = context.textFile(path)
    val tokens = fileInput.map(line => line.split(",")).filter(_.size == tagSize). map(
      y => new TagStep(y(0),y(1),y(2),y(3)) )
    val result:RDD[(String,Iterable[TagStep])] = tokens.keyBy( _.session_id.trim).groupByKey
    result
  }

  /**
    * The function returns top n records sorted by time
    * @param input is RDD grouped by sessions
    * @param numTags number of records to be returns
    * @return The result has top records which are sorted by time
    */
  def mostRecentTags( input:RDD[(String,Iterable[TagStep])], numTags:Int): Array[List[TagStep]] = {

    val values:RDD[(String,(Long,List[TagStep]))] = input.mapValues(iter =>
      (iter.toList(0).getTimeLong,iter.toList)).sortBy( _._1,false)
    val limited = values.take(numTags).map(x => x._2._2)
    limited
  }

  /**
    * The function returns steps of specified size
    * @param input is RDD grouped by sessions
    * @param stepSize size of the step that needs to be filtered out
    * @return return the result of all the tag records to specified size
    */
  def getStepsList( input:RDD[(String,Iterable[TagStep])], stepSize:Int  ) : Array[List[TagStep]] = {
    val filtered:RDD[Iterable[TagStep]] = input.map(_._2.filter( _.isStep) )
    val resultFil = filtered.map( x => x.toList).filter(_.size == stepSize)
    resultFil.collect
  }


}