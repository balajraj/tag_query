package com.booking



case class TagStep(timestamp:String, session_id:String, step_id:String, step_details:String)
{
    val format = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
    def getTimeLong : Long = {

      format.parse(timestamp).getTime
    }

    def getStepId : Int = {
       val stepIds = step_id.split(" ")
       return stepIds(1).toInt
    }
}
//case class TagData(time:Long, id:Long, steps:List[TagStep])

