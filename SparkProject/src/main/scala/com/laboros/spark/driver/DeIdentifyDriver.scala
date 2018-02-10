package com.laboros.spark.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DeIdentifyDriver {
  def main(args: Array[String]): Unit = {
     val sparkConf:SparkConf=new SparkConf();
//                .setAppName("WordCountDriver")
//                .setMaster("local")
//                .set("spark.submit.deployMode","client");
    
    //Step: 2 : Create Context
    val sc:SparkContext = new SparkContext(sparkConf);
    
    val input=sc.textFile(args(0), 20);
    
    val deIdentify=input.map(iLine=>iLine.split(",")).map(input=>deIdentifyArray(input));
    
    deIdentify.take(5).foreach(println);
  }
  
  def deIdentifyArray(input:Array[String]):String = {

    val iDeIdentifyCols:Array[Int] = Array(1,3,5,6);
    val iLen=input.length;
    
    val output:StringBuffer = new StringBuffer();
    for(i<- 0 until iLen)
    {
      val columnDeIdentify = checkColumnDeIdentify(i, iDeIdentifyCols);
      if(columnDeIdentify)
      {
        output.append(deIdentify(input(i)));
      }else{
        output.append(input(i));
      }
      output.append(",");
    }
    output.toString();
  }
  
  def checkColumnDeIdentify(i:Int, colums:Array[Int]):Boolean ={
    
    for(j<- colums){
      if(i == j){
        return true;
      }
    }
    false;
  }
  
  def deIdentify(column:String):String="XXXXX";
}