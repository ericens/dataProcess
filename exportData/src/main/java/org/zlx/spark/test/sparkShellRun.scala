package org.zlx.spark.test

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by @author linxin on 27/07/2018.  <br>

  hdfs文件
/tmp/stu.text

a,math,99
a,english,90
a,chinese,60
b,math,98
b,english,93
b,chinese,62
c,math,100
c,english,70
c,chinese,59



  *
  */
object sparkShellRun {

  var spark:SparkSession=null
  var sc:SparkContext=null
  var inputPath:String="/tmp/stu.text"

  //run in spark-shell
  def main(args: Array[String]): Unit = {

  }

  /**  TODO scala入门 https://blog.csdn.net/lovehuangjiaju/article/details/46984575
    *
    */
  def sacalaIn(): Unit ={


  }





  /**
    * 自定义输出格式    https://vimsky.com/article/3743.html
    * dataSet转RDD
    * dataSet转DF
    *  https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes
    * tested: ok
    */
  def wordCount(): Unit = {

    var outputDir = "output_dir_wordcount1";
    sc.textFile(inputPath).flatMap(str => str.split(",")).map(s => (s, 1)).reduceByKey(_ + _).saveAsTextFile("output_dir_wordcount1")

  }

  def testDataset(): Unit = {
    val dataSet: Dataset[String] = spark.read.textFile(inputPath) //dataSet: org.apache.spark.sql.Dataset[String] = [value: string

    var threadColDataSet: Dataset[(String, String, String)] = spark.read.textFile(inputPath).map(str => str.split(",")).map(a => (a(0), a(1), a(2)))
    threadColDataSet.printSchema()
    //to dataFrame
    var df=threadColDataSet.toDF("name","class","score");
    df.select("score").show()

    //重新定义一个 schema
    case class stu(name:String,className:String,score:Int) extends Serializable
    var dataset22 = spark.read.textFile(inputPath).map(str => str.split(",")).map(a => stu(a(0), a(1), a(2).toString.trim.toInt))
    dataset22.select( "name").show()
    dataset22.createOrReplaceTempView("tmp")
    spark.sql("select * from tmp where score> 90").show(100,false)


    //schema
    val structFieldS = List(
      StructField("name", StringType, true),
      StructField("classId", StringType, false),
      StructField("scores", IntegerType, false)
    )
    var schemaxx = threadColDataSet.schema
    schemaxx.printTreeString()
    structFieldS.foreach(
      item => schemaxx = schemaxx.add(item)
    )
    schemaxx.printTreeString()
    schemaxx.drop(0).drop(1)
    schemaxx.printTreeString()
  }

  /**
    * RDD 的操作 ，RDD 的每一行是String,可以进行split等拆分。
    * 根据拆分，组装成自定义对象Student
    * tested: ok
    */
  def testRddObject(): Unit=
  {
    //定义匹配类
    case class Student(name: String,classId:String, score: Long)
    //local
    var personRDD=spark.sparkContext.textFile("file:////Users/ericens/tools/spark-2.3.0-bin-hadoop2.7/examples/src/main/resources/people.txt").map(_.split(",")).map(attributes => Student(attributes(0),attributes(1), attributes(2).trim.toInt))

    //hdfs, string转化为一个student对象
    var personRDD2=spark.sparkContext.textFile("/tmp/stu.text").map(_.split(",")).map(attributes => Student(attributes(0), attributes(1),attributes(2).trim.toInt)).map(stu=> stu.name).saveAsTextFile("output1")


    var x=spark.sparkContext.textFile("/tmp/stu.text").map(_.split(",")).map(attributes => Student(attributes(0), attributes(1),attributes(2).trim.toInt))

    //dataset => rdd
    val dataSet: Dataset[String] = spark.read.textFile(inputPath)
    var rdd=dataSet.rdd.flatMap( _.split(" ") ).filter(_ != "")     //df: org.apache.spark.rdd.RDD[String]

    spark.sparkContext.textFile("/user/ericens/kv1.txt")  //是一个RDD,每行是String，:  org.apache.spark.rdd.RDD[String]
      .map(_.split(","))   //org.apache.spark.rdd.RDD[Array[String]]       //RDD，每行是Array, Array是String
      .map(attributes => Student(attributes(0),attributes(1), attributes(2).trim.toInt))  //RDD, 每行是Person,org.apache.spark.rdd.RDD[Person]
    personRDD.take(10)



    spark.sparkContext.textFile("/user/ericens/kv1.txt").flatMap(_.split(",")).map(s => (s,1) ).reduceByKey( (v1,v2)=>v1 + v2).saveAsTextFile("test")
    var countx: RDD[(String,Int)]=spark.sparkContext.textFile("/user/ericens/kv1.txt")  //是一个RDD,每行是String，:  org.apache.spark.rdd.RDD[String]
      .flatMap(_.split(","))   //org.apache.spark.rdd.RDD[Array[String]]       //RDD，每行是Array, Array是String
      .map(s => (s,1) ).reduceByKey( (v1,v2)=>v1 + v2)
    countx.saveAsTextFile("test")



  }


  /**
    * 1. DataFrame每一行的类型固定为Row，只有通过解析才能获取各个字段的值
    * 2. 操作时候，每一行得解析一遍
    * 3. 支持sql
    */
  def testdataFrame(): Unit ={

    var threadColDataSet: Dataset[(String, String, String)] = spark.read.textFile(inputPath).map(str => str.split(",")).map(a => (a(0), a(1), a(2)))
    var df=threadColDataSet.toDF("name","class","score");
    df.select("score").show()

    //每一行自己转
    df.map(
      line =>{
        val col1=line.getAs[String]("name")
        val col2=line.getAs[String]("class")
        col1+"xxxx"+col2
      }).show()

    //支持sql
    df.createOrReplaceTempView("tmp")
    spark.sql("select * from tmp where score> 90").show(100,false)

    //注意元组的定义
    sc.textFile("/tmp/stu.text").flatMap(str => str.split(",")).map(s => (s, 1)).reduceByKey( _ + _).map(
      e=>
      {val (k,v)=e;
        k+":"+v
      }
    ).saveAsTextFile("output_dir_wordcount2")
    //    396val:3
    //    114:1
    //    224val:2


    //注意元组的定义
    sc.textFile("/tmp/stu.text").flatMap(str => str.split(",")).map(s => (s, 1)).reduceByKey( _ + _).map( ( (kv:(String,Int)) => {
      kv._1.concat(":xxx:"+kv._2.toString)
    }  )).saveAsTextFile("output_dir_wordcount3")


    //TODO  覆盖写  RDD's saveAsTextFile does not give us the opportunity to do that (DataFrame's have "save modes" for things like append/overwrite/ignore).
    // You'll have to control this prior before (maybe delete or rename existing data) or afterwards (write the RDD as a diff dir and then swap it out).
    //def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame


  }



  /**
    * RDD 的操作 ，RRD 转 dataFrame
    * tested: ok
    */

  def rddToDataFrame():Unit={
    // 定义schema
    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("classId", StringType, false),
        StructField("scores", IntegerType, false)
      )
    )

    // 将RDD映射到rowRDD上
    val rowRDD: RDD[Row]= sc.textFile("/tmp/stu.text").map(str=> (str.split(","))).map( strArray => Row(strArray(0),strArray(1),strArray(2).trim.toInt))
    var dataFrame=spark.sqlContext.createDataFrame(rowRDD,schema)
    dataFrame.write.mode(SaveMode.Overwrite).save("output")
    dataFrame.select("name","scores").take(10)



    sc.textFile(inputPath)
      .flatMap(str => str.split(",")).map(s => (s, 1))        //org.apache.spark.rdd.RDD[(String, Int)]
      .reduceByKey(_ + _)
      .saveAsTextFile("output_dir_wordcount1")

    sc.textFile(inputPath).flatMap(str => str.split("_")).map(s => (s, 1)).reduceByKey(_ + _)  //org.apache.spark.rdd.RDD[(String, Int)]


  }



  /**
    * DataFrame 的操作
    * //运行在spark-shell里面
    * hadoop fs -put people.json
    */
  def jsonRead():Unit = {
    //jsonDf: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
    var jsonDf=spark.read.json("people.json");
    jsonDf.printSchema()
    jsonDf.show()

    var nameDf=jsonDf.select("name")
    nameDf.show()


    var filterDf=jsonDf.filter(row=>{
      println(row)
      !row.isNullAt(0) && row.getAs[Long](0) > 18
    } )
    filterDf.show()
    /*
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
     */

    val df = spark.createDataFrame(Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")  //[id: int, v1: double ... 1 more field]
    var se=df.select("id","v1")


  }




  /**
    * 倒排索引:https://blog.csdn.net/sysmedia/article/details/70049464
    */
  def invertedIndex():Unit ={
    /*
    原始数据
cx1|a,b,c,d,e,f
cx2|c,d,e,f
cx3|a,b,c,f
cx4|a,b,c,d,e,f
cx5|a,b,e,f
cx6|a,b,c,d
cx7|a,b,c,f
cx8|d,e,f
cx9|b,c,d,e,f

结果数据
d|cx1,cx2,cx4,cx6,cx8,cx9
e|cx1,cx2,cx4,cx5,cx8,cx9
a|cx1,cx3,cx4,cx5,cx6,cx7
b|cx1,cx3,cx4,cx5,cx6,cx7,cx9
f|cx1,cx2,cx3,cx4,cx5,cx7,cx8,cx9
c|cx1,cx2,cx3,cx4,cx6,cx7,cx9
     */

    val source = scala.io.Source.fromFile("test.md").getLines.toArray
    val cxRDD0 = sc.parallelize(source)                        /* spark单机读取数据 */

    cxRDD0.flatMap {
      lines =>
        val line = lines.split("\\|", -1)                      /* 拆分数据，以竖杠为拆分条件 */
        line(1).split(",", -1).map {                           /* 再对拆分后的数据，进行第二次拆分 */
          v =>
            (v, line(0))                                       /* 拼接数据 */
        }
    }.groupByKey()                                             /* 分组 */
      .sortBy(_._1,true)                                       /* 排序 */
      .foreach(x => println(s"${x._1}|${x._2.mkString(",")}"))   /* 格式化输出 */


    //TODO 方法二待测试sa
    val words = sc.parallelize(source)
      .map(file=>file.split("\t"))
      .map(item =>{
        (item(0),item(1))
      })
      .flatMap(file => {
        var map = mutable.Map[String,String]()
        val words = file._2.split(" ").iterator
        val doc = file._1
        while(words.hasNext){
          map+=(words.next() -> doc)
        }
        map
      })

    //save to file
    words.reduceByKey(_+" "+_).map(x=>{
      x._1+"\t"+x._2
    }).saveAsTextFile("hdfs://master:8020/test3")


  }



  /**
    * 读取本地磁盘文件
    */
  def readFromLocal( ): Unit ={
    var fileSource=scala.io.Source.fromFile("derby.log")

    //TODO  注意 遍历一次就没有了
    val strIterator = fileSource.getLines()
    strIterator.foreach( print _ )


    val list = fileSource.getLines().toList
    //是一个list, 元素是map
    list.flatMap(str => str.split(" ")).map(s => (s, 1))
    //元素是map,才转成 Map
    var map=list.flatMap(str => str.split(" ")).map(s => (s, 1)).toMap

    //遍历
    for( k <- map){
      println(k._1,k._2)
    }




  }

}
