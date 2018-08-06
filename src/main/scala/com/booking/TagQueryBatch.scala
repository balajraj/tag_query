package com.booking

import org.apache.spark.{ SparkContext}


object TagQueryBatch {

  import net.liftweb.json._
  implicit val formats = DefaultFormats

  def mostRecentTags( path:String, context:SparkContext,numTags:Int): Unit = {
    println("Entering mostRecentTags")
    val fileInput = context.textFile(path)
    val tokens = fileInput.map(line => line.split(",")).map( y => {new TagStep(y(0),y(1),y(2),y(3))})
    tokens.collect().foreach(println)
    //fileInput.flatMap(_.split(",")).map(x => println("%%%%%%%%%%%%%%%%%%%% h4llo "+x))
    println("Leaving mostRecentTags")
  }


  def main(args : Array[String]): Unit = {

  }
}