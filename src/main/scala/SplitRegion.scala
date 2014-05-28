
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor }
import org.apache.hadoop.hbase.HBaseConfiguration

object SplitRegion extends App {

  override def main(args: Array[String]) = {
    create(args)
//    getSplitKeys(18, true)
  }

  def create(args: Array[String]) = {
    if (args.length != 5) {
      println("""enter 5 parameters for "cluster.distributed[1: true, 0: false]", "zookeeper.quorum[hadoop:2181]",  "table[messages]", "family[a]", "splitCount[18]" """)
      sys.exit(-1)
    }
    val conf = HBaseConfiguration.create()
    conf.set("cluster.distributed", if ("1".equals(args(0))) "true" else "false")
    conf.set("zookeeper.quorum", args(1))
    val admin = new HBaseAdmin(conf)
    val tableBytes = Bytes.toBytes(args(2))
    val familyBytes = Bytes.toBytes(args(3))
    val splitNum = Integer.parseInt(args(4))
    if (admin.tableExists(tableBytes)) {
      val tableDesc = admin.getTableDescriptor(tableBytes)
      if (tableDesc.getFamily(familyBytes) == null) {
        // target family does not exists, will add it.
        println("Table Exists, add family !!!")
        admin.addColumn(tableBytes, new HColumnDescriptor(familyBytes))
      } else {
        // existing table is OK, no modifications run.
        println("Table Exists !!!")
      }
    } else {
      val tableDesc = new HTableDescriptor(tableBytes)
      val familyDesc = genColumnFamily(familyBytes, 1)
      tableDesc.addFamily(familyDesc)
      if (splitNum > 1) {
        val splitPoints = getSplitKeys(splitNum);
        admin.createTable(tableDesc, splitPoints)
      } else {
        admin.createTable(tableDesc)
      }
    }
  }

  private def genColumnFamily(family: Array[Byte],  columnMaxVersion: Int): HColumnDescriptor = {
     val familyDesc: HColumnDescriptor = new HColumnDescriptor(family)
      .setInMemory(false)
      .setMaxVersions(columnMaxVersion);
    familyDesc
  }

  private def getSplitKeys(splitNum: Int, isPrint: Boolean = false):Array[Array[Byte]] = {
    val list = collection.mutable.ListBuffer.empty[Array[Byte]]
    for (i <- 1 until splitNum) {
      val keyBytes = collection.mutable.ListBuffer.empty[Byte]
      keyBytes ++= Bytes.toBytes(padNum(i, 2))
      val zeroByte:Byte = Bytes.toBytes(0).tail(0)
      for (j <- 0 until 24) {
          keyBytes += zeroByte
      }
      val bytes = keyBytes.toArray
      if (isPrint) println(s" $i ${Bytes.toString(bytes)} ${renderBytes(bytes)}")
      list.append(bytes)
    }
    list.toArray
  }

  def padNum(l: Int, howLong: Int):String = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString.substring(0, howLong)

  def renderBytes(bytes: Array[Byte]): String = {
    bytes.map("%02x".format(_)).mkString
  }
}