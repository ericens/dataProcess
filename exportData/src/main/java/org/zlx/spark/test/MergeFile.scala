package org.zlx.spark.test

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by @author linxin on 2018/9/11.  <br>
  *
  */
object MergeFile {



  def main(args: Array[String]): Unit = {
    //Spark master位置、应用程序名称、 Spark安装目录和Jar存放位置。




  }
  def main2(args:Array[String]):Unit={
    val Seq(model, input, output) = args.toSeq

    val spark = SparkSession.builder().master(model).appName(Option("mergeFile").getOrElse("SPARK-mergeFile")).getOrCreate()
    var Unit=spark.sparkContext.addFile(input);

    var parquetFileDF = spark.read.parquet("/user/hive/warehouse/ad.db/ad_dmp_device_info/data_date=20180905/000031*")
    println(parquetFileDF.count());
    parquetFileDF = spark.read.parquet("/user/hive/warehouse/ad.db/ad_dmp_device_info/data_date=20180905/000032*")
    println(parquetFileDF.count());

    //小文件合并
    parquetFileDF = spark.read.parquet("/user/hive/warehouse/ad.db/ad_dmp_device_info/data_date=20180905/00003*")
    println(parquetFileDF.count());
    parquetFileDF.write.save("/tmp/00003")
    //自动拆分文件
    //196.6 M  589.8 M  /tmp/00003/part-00000-9fa930cd-c884-4342-a7d4-aa4610ca21a6.snappy.parquet
    //199.4 M  598.3 M  /tmp/00003/part-00001-9fa930cd-c884-4342-a7d4-aa4610ca21a6.snappy.parquet


    //保存一个文件
    parquetFileDF.coalesce(1).write.save("/tmp/00003")

    parquetFileDF = spark.read.parquet("/tmp/00003")
    println(parquetFileDF.count());


  }
  def wordCount(args: Array[String]):Unit={
    val Seq(model, input, output) = args.toSeq
    val spark = SparkSession.builder().master(model).appName(Option("mergeFile").getOrElse("SPARK-mergeFile")).getOrCreate()
    var ssds: Dataset[String] = spark.read.textFile("/user/hive/warehouse/ad.db/ad_dmp_device_info/data_date=20180905/000031*")
    var wdS=ssds.rdd.flatMap( toArrayStr )
    var xx=wdS.map( (word) =>{ (word,1)})

    var parquetFileDF: sql.DataFrame = spark.read.parquet("/user/hive/warehouse/ad.db/ad_dmp_device_info/data_date=20180905/000031*")

    val tuple = (1, false, "Scala")




    parquetFileDF.coalesce(1).write.save("/tmp/00005")




  }
  def toArrayStr(str:String):Array[String]={
    str.split(" ")
  }

  //scala元组
  def f(ab:(String, Int),cd:(String,Int)) : (String,Int) = {
    val (a, b) = ab
    val (c, d) = cd
    (a,d)
  }




}












