package com.itcode.demo

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author : code1997
 * @date : 2022/1/7 0:06
 */
object Executor2 {

  def main(args: Array[String]): Unit = {
    //创建服务器端socket
    val socket: ServerSocket = new ServerSocket(9999)
    //等待客户端的连接
    val client: Socket = socket.accept()
    println("服务器2启动，等待客户端的连接...")
    val is: InputStream = client.getInputStream
    val ois = new ObjectInputStream(is)
    val task: SubTask = ois.readObject().asInstanceOf[SubTask]
    val result: List[Int] = task.compute()
    println("计算节点，计算完成，结果为：" + result)
    ois.close()
    is.close()
    client.close()
    socket.close()
    println("服务器已经关闭...")
  }

}
