package com.laboros.spark.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils

object JoinDriver {
  def main(args: Array[String]): Unit = {
    
    
    val sparkConf:SparkConf=new SparkConf()
                .setAppName("WordCountDriver")
                .setMaster("local")
                .set("spark.submit.deployMode","client");
    
    //Step: 2 : Create Context
    val sc:SparkContext = new SparkContext(sparkConf);
    val custInput = sc.textFile(args(0), 10);
    val txnsInput = sc.textFile(args(1),10);
    
    val custPair = custInput.map(iLine =>iLine.split(",")).map(c=>(c(0),"CUSTS\t"+c(1)));
    
    val txnPair = txnsInput.map(iLine=>iLine.split(",")).map(t=>(t(2),"TXNS\t"+t(3)));
    
    
//    var name ="";
    val simpleJoinPair = custPair.join(txnPair).reduceByKey((ct,tt)=>{
      
      val c:Array[String]=ct._2.split("\t");
      val t:Array[String]=tt._2.split("\t");
      
      if(c(0).equals("CUSTS")){
       
        (ct._2,tt._2)
      }else{
        if(c.length==2 && t.length==2)
        {
        (ct._1,ct._2+tt._2)
        }else{
          (ct._2,tt._2)
        }
      }
      
      }
    );
    
    
    simpleJoinPair.collect().foreach(println);
    
  }
}