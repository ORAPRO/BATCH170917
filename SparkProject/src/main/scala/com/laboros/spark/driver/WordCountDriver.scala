package com.laboros.spark.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils

//com.laboros.spark.driver.WordCountDriver

object WordCountDriver {
  
  def main(args: Array[String]): Unit = {
    
    //Step:1 Create Spark Configuration
    
    val sparkConf:SparkConf=new SparkConf();
//                .setAppName("WordCountDriver")
//                .setMaster("local")
//                .set("spark.master.deployMode","client");
    
    //Step: 2 : Create Context
    val sc:SparkContext = new SparkContext(sparkConf);
    
    //Step: 3 : Create inputRDD --> Load the data or refe a collection
    
    val input = sc.textFile(args(0),10);//hdfs://user/trainings/WordCount.txt
    
    //input --> {(DEER RIVER RIVER),
        //        (RIVER CAT BEAR),
    //            (DEER RIVER CAT),
    //            (RIVER CAT CAT) 
    //} 
    
    val words=input.flatMap(iLine=> iLine.split(" "));
   
    val addOneToEachWord = words.map(word=>(word,1));
    
    val wc = addOneToEachWord.reduceByKey(_+_);
    wc.saveAsTextFile(args(1));
  }
  
}