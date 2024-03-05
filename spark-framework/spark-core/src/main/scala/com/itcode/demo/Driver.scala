package com.itcode.demo

import java.io.{InputStream, ObjectOutputStream, OutputStream}
import java.net.{ServerSocket, Socket}

/**
 * driver当作客户端，executor当作服务器端，用于接受数据，进行计算。
 * 1.在网络中传输对象必须可以序列化。
 * 2.在服务器端我们接收到对象之后，必须直到传递过来的是什么类，有什么方法，以及方法的执行流程。
 * 3.如果我们模仿分布式的情况下，那么就需要考虑数据是是如何划分，逻辑应该是一致的。
 *
 * @author : code1997
 * @date : 2022/1/7 0:05
 */
object Driver {

  def main(args: Array[String]): Unit = {
    val client1: Socket = new Socket("localhost", 8888)
    val client2: Socket = new Socket("localhost", 9999)
    val task = new Task()

    val os1: OutputStream = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(os1)

    val os2: OutputStream = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(os2)

    val subTask1 = new SubTask()
    subTask1.logic = task.logic
    subTask1.datas = task.datas.take(2)
    objOut1.writeObject(subTask1)

    val subTask2 = new SubTask()
    subTask2.logic = task.logic
    subTask2.datas = task.datas.take(2)
    objOut2.writeObject(subTask2)

    //关闭流
    objOut1.flush()
    objOut2.flush()
    objOut1.close()
    objOut2.close()
    client1.close()
    client2.close()
    print("客户端数据发送完毕")
  }

}
