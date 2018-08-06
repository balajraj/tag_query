package com.booking

case class TagStep(time:String, stepId:String, stepLabel:String, tagDetail:String)
case class TagData(time:Long, id:Long, steps:List[TagStep])

