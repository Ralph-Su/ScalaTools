/**
  * @Author: suwenjin
  * @Description: 使用Scala执行Shell命令
  * @Time: 2022/5/24 9:30 PM
**/
object shellExample extends App{
  import sys.process._
  //shell命令最后加上.!表示执行命令，也可是把执行结果赋值给一个不可变变量
  //.!返回结果为int，0表示成功，.!!返回结果为打印的内容，为string
  "ls -l".! //执行命令，并把结果打印到控制台上
}
