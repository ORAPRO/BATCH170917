package com.laboros.spark.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils

object WeatherDriver {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("WeatherDriver")
      .setMaster("local").set("spark.submit.deployMode", "client");

    val sc: SparkContext = new SparkContext(sparkConf);

    //load the inputRDD

    val inputRDD = sc.textFile(args(0), 10);
    //27516 20140101  2.424 -156.61   71.32   
    //-16.6   -18.7   -17.7   -17.7     0.0     
    //0.00 C   -17.8   -19.4   -18.7    83.8   
    //73.5    80.8 -99.000 -99.000 -99.000 -99.000 -99.000
    //-9999.0 -9999.0 -9999.0 -9999.0 -9999.0
    
    //year -- > date,,max  --> TUPLE --> (year,(date,max))
    
    val tupels=inputRDD.map(iLine=> {
    val date=StringUtils.substring(iLine, 6, 14);
    val year=StringUtils.substring(date, 0, 4);
    val max_temp=StringUtils.substring(iLine, 38,45);
    (year,(date,max_temp.trim().toFloat));
    })

    val computeMaxTemp = tupels.reduceByKey((tv1,tv2)=>{
      if(tv1._2 == -9999.0)
      {
        tv2
      }else if(tv2._2 == -9999.0)
      {
        tv1
      }
      if(tv1._2>tv2._2)
      {
        tv1
      }else{
        tv2
      }
    });
    computeMaxTemp.collect().foreach(println)
  }
}