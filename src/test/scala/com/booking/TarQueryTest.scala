package com.booking


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Assertions._

class TarQueryTest extends FunSuite with BeforeAndAfterAll {


  private var sparkConf: SparkConf =  _
  private var sc:SparkContext = _

  private var groupQuery:RDD[(String,Iterable[TagStep])] = _

  override def beforeAll(): Unit = {
    sparkConf = new SparkConf().setAppName("unit-test").setMaster("local")
    sc = new SparkContext(sparkConf)
    groupQuery =  TagQueryBatch.groupSteps("src/test/resources/", sc)
  }

  test( "most Recent Tag should return the tag sorted on time by latest ") {

    val result:Array[List[TagStep]] = TagQueryBatch.mostRecentTags(groupQuery,1)
    val tagSteps:List[TagStep] = result(0)
    assert(tagSteps(0).step_id == "STEP 1")
    assert(tagSteps(1).step_details == "IFAR")
    assert(tagSteps(2).step_id == "STEP 2")

  }

  test (" given the step size the getStepList should return steps equal to step size") {

    val stepList2:Array[List[TagStep]] = TagQueryBatch.getStepsList(groupQuery,2)
    val tagSteps:List[TagStep] = stepList2(0)
    assert(tagSteps(0).step_id == "STEP 1")
    assert(tagSteps(1).step_id == "STEP 2")

    val stepList4:Array[List[TagStep]] = TagQueryBatch.getStepsList(groupQuery,4)
    assert(stepList4.size == 0)

    val stepList6:Array[List[TagStep]] = TagQueryBatch.getStepsList(groupQuery,6)
    val tagSteps1:List[TagStep] = stepList6(0)
    assert(tagSteps1(0).step_id == "STEP 1")
    assert(tagSteps1(1).step_id == "STEP 2")
    assert(tagSteps1(2).step_id == "STEP 3")
    assert(tagSteps1(3).step_id == "STEP 4")
    assert(tagSteps1(4).step_id == "STEP 5")
    assert(tagSteps1(5).step_id == "STEP 6")

  }

  override def afterAll(): Unit = {
    sc.stop()
  }



}