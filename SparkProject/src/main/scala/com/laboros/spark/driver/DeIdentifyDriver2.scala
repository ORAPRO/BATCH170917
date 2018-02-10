package com.laboros.spark.driver

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DeIdentifyDriver2 {
  
  def main(args: Array[String]): Unit = {
    
    val sparkConf:SparkConf = new SparkConf().setAppName("DeIdentifyDriver2");
    val sc:SparkContext = new SparkContext(sparkConf);
    
    //Load the input data
   val input:String = args(0);
   
   val inputRDD = sc.textFile(args(0), 8);
   
   //11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,1111111111,M,Diabetes,78

   //Identify which columns need to deIdentify.
   //Array(0)--11111
   //4,5,6
   
   val deIdentifyColumns:Array[Int] = Array(4,5,6);
   
   val columnsRDD:RDD[Array[String]]=inputRDD.map(iLine=>StringUtils.split(iLine, ","));

      //Array(0)--11111
      //Array(1)--bbb1
     //Array(2)--12/10/1950 
      //Array(3)--1234567890
      //Array(4)--bbb1@xxx.com
      //Array(5)--,1111111111
      //Array(6)--,M,
      //Array(7)-- Diabetes,
       //Array(8) -- 78
   
   val deIdentifyData = columnsRDD.map(as=>{
     
     val sb:StringBuffer = new StringBuffer();
     val al:Int = as.length;
     
     for(iColIdx <- 0 until al)
     {
       val deIdentifyCol=checkToDeIdentify(iColIdx, deIdentifyColumns);
       if(deIdentifyCol)
       {
         sb.append(deIdentify(as(iColIdx)));
       }else{
         sb.append(as(iColIdx));
       }
       sb.append(",");
       
     }
    sb.toString(); 
   })
   deIdentifyData.take(10).foreach(println);
  }
  
  
  def checkToDeIdentify(iColIdx:Int, deIdentifyColumns:Array[Int]):Boolean = 
  {
    for(i <- deIdentifyColumns)
    {
      if(iColIdx == i-1){
        return true;
      }
    }
    false;
  }
  
  
  def deIdentify(column:String) = "XXXXX-XXXX";
  
  
}