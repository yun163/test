package test

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.util.Map.Entry
import java.util.Iterator
import java.io.File

object TestFs {
  def main(args: Array[String]) {
    val conf = new Configuration
    //    conf.addResource(new File("/work/software/hadoop-1.2.1/conf/core-site.xml").toURI().toURL())
    //    conf.addResource(new File("/work/software/hadoop-1.2.1/conf/hdfs-site.xml").toURI().toURL())
//    conf.addResource("core-site.xml")
//    conf.addResource("hdfs-site.xml")
    conf.set("fs.default.name", "hdfs://weichaoli:54310")
    val it: Iterator[Entry[String, String]] = conf.iterator()
    while (it.hasNext) {
      val entry = it.next()
      System.out.println("%s -> %s".format(entry.getKey(), entry.getValue))
    }
    val fs = FileSystem.get(conf) // todo allow passing in all conf?
    fs.create(new Path("/snapshot/", "ttt"))
    println("Hello, world!")
  }
}
