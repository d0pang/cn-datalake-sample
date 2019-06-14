package cn.datalake.utils

import java.io.File
import java.security.PrivilegedAction

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

/**
  * describe: 获取HBase Conncetion工具类
  * creat_date: 2019/6/14
  */
object HBaseUtil {

  /**
    * 获取Kerberos环境下的HBase连接
    * @param confPath
    * @param principal
    * @param keytabPath
    * @return
    */
  def getHBaseConn(confPath: String, principal: String, keytabPath: String): Connection = {
    val configuration = HBaseConfiguration.create
    val coreFile = new File(confPath + File.separator + "core-site.xml")
    if(!coreFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
      configuration.addResource(in)
    }
    val hdfsFile = new File(confPath + File.separator + "hdfs-site.xml")
    if(!hdfsFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hdfs-site.xml")
      configuration.addResource(in)
    }
    val hbaseFile = new File(confPath + File.separator + "hbase-site.xml")
    if(!hbaseFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hbase-site.xml")
      configuration.addResource(in)
    }

    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Connection] {
      override def run(): Connection = ConnectionFactory.createConnection(configuration)
    })
  }


  /**
    * 获取非Kerberos环境的Connection
    * @param confPath
    * @return
    */
  def getNoKBHBaseCon(confPath: String): Connection = {

    val configuration = HBaseConfiguration.create
    val coreFile = new File(confPath + File.separator + "core-site.xml")
    if(!coreFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
      configuration.addResource(in)
    }
    val hdfsFile = new File(confPath + File.separator + "hdfs-site.xml")
    if(!hdfsFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hdfs-site.xml")
      configuration.addResource(in)
    }
    val hbaseFile = new File(confPath + File.separator + "hbase-site.xml")
    if(!hbaseFile.exists()) {
      val in = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hbase-site.xml")
      configuration.addResource(in)
    }

    val connection = ConnectionFactory.createConnection(configuration)
    println("_---------------------" + connection.getAdmin.listTableNames().size)
    connection
  }

}
