package com.prerna.retail

import org.apache.spark.sql.SparkSession
import java.lang.Float

object StoreSales {
  def main (args : Array[String]){
    
    //check if the number of arguments are correct
    if (args.length < 2){ 
      System.err.println("Usage: product Sales <Input File> <Output File>");
      System.exit(1);
    }
    
    val spark = SparkSession
            .builder
            .appName("StoreSales")
            .getOrCreate()
            
    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.map(line => {
      val tokens = line.split("\\t")
      (tokens(2), Float.parseFloat(tokens(4)))
    })
    .reduceByKey(_+_)
    
    //Store_category Sales-Val
    result.saveAsTextFile(args(1))
    
    spark.stop()
    
    
  }
}
