package com.booking


case class TagStep(timestamp:String, session_id:String, step_id:String, step_details:String)
{
    val format = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
    def getTimeLong : Long = {
      format.parse(timestamp).getTime
    }

    def isStep : Boolean = {
       val result = if(step_id.contains("STEP") ) true else false
       result
    }
}


