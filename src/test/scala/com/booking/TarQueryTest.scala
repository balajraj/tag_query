package com.booking

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TarQueryTest extends FunSuite with BeforeAndAfterAll {


  private var sparkConf: SparkConf =  _
  private var sc:SparkContext = _


  override def beforeAll(): Unit = {
    sparkConf = new SparkConf().setAppName("unit-test").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  test( "most Recent Tag should return the num of Most recent tags ") {
    //println("dir current" + new File(".").getCanonicalPath());
    TagQueryBatch.mostRecentTags("src/test/resources/", sc,2)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }



}