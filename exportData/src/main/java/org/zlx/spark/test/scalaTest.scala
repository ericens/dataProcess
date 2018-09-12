package org.zlx.spark.test

/**
  * Created by @author linxin on 28/07/2018.  <br>
  *
  */
class scalaTest {


  def main(args: Array[String]): Unit = {
    charArray
    toMap
    println(square(2))
    println(square2(2))
    println(square3(2))
  }



  def square(i:Int)={i*i}
  def square2(i:Int):Int={i*i}
  def square3(i:Int):Int={i*i}


  def charArray(): Unit = {
    var intList = List(1, 2, 3, 4, 5, 6)
    intList.foreach(
      println _
    )

    var strList = List("12", "34", "56", "78")
    strList.foreach(
      println _
    )
    println("..............................")

    //作用在字符串 list2 上ok，list报错。item(0) 是取的字符串在0字母。
    var tuple = strList.map(item => {
      (item(0), item(1))
    })
    tuple.foreach(
      println(_)
    )


    tuple=strList.map(item=>  (item(0),item(1)))
    tuple.foreach(
      println( _ )
    )
    println(tuple.mkString(","))

  }

  def toMap(): Unit = {

    val list = List("this", "maps", "string", "to", "length")
    var m=list.map ( t => t-> t.length).toMap
    println (m.contains("this"))
    println (m.contains("thix"))


    var initMap=Map(1-> 2,3->4)
    initMap.foreach(
      println( _ )
    )

   var m2=list.map(t => t(0) -> t(1) ).toMap
   println (m2.mkString(","))

    var m3=list.map(t => (t(0) , t(1)) ).toMap
    println (m3.mkString(","))


    //todo 报错
//    var m4=list.map(t => Map(t(0) -> t(1)) ).toMap
//    println (m4.mkString(","))

  }
}
