package com.laboros.spark.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils

object JoinDriver2 {
  
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkContext(new SparkConf().setAppName("JoinDriver2"));
    
    val inputCustRDD = sc.textFile(args(0), 10);
    val inputTxnRDD = sc.textFile(args(1), 10);
    
    
    //4000001,Kristina,Chung,55,Pilot
    
    val custPair = inputCustRDD
    .map(iLine => StringUtils.splitPreserveAllTokens(iLine, ",")) // Array---> 
    .map(a=>(a(0),a(1)))
    .filter(t=> StringUtils.isNotEmpty(t._1) && StringUtils.isNotEmpty(t._2));  
    
    
    //00000000,06-26-2011,4007024,040.33,
    //Exercise & Fitness,Cardio Machine Accessories,Clarksville,Tennessee,credit

    val txnPair = inputTxnRDD
     .map(iLine => StringUtils.splitPreserveAllTokens(iLine, ",")) // Array---> 
    .map(a=>(a(2),a(3)))
    .filter(t=> StringUtils.isNotEmpty(t._1) && StringUtils.isNotEmpty(t._2));  
    
    /**
     * (4003138,(Rhonda,105.72))
        (4003138,(Rhonda,059.77))
        (4003138,(Rhonda,157.26))
        (4003138,(Rhonda,170.61))
        (4003138,(Rhonda,040.25))
        (4003138,(Rhonda,063.95))
        (4003138,(Rhonda,060.93))
        (4003138,(Rhonda,148.24))
        (4003138,(Rhonda,174.92))
        
         (4003138,(Rhonda,105.72)(Rhonda,059.77))
     * 
     */
    
    val joinRDD = custPair.join(txnPair);
    
    val outputRDD=joinRDD.reduceByKey((t1,t2) => {
//      var count:Int = count+1;
      var sum:Float=t1._2.toFloat+t2._2.toFloat;
      var name:String =t1._1;
      (name,sum.toString())
    })
    
    outputRDD.collect()
    .foreach(t=>
      {
        println(t._2._1+"\t"+t._2._2)
        });
    
  }
}