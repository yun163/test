import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import org.slf4j.{ Logger, LoggerFactory }
import org.apache.log4j.PropertyConfigurator
import org.apache.commons.io.FileUtils

object HdfsFileClear extends App {
  override def main(args: Array[String]) {
    if (args.length < 1) {
      sys.error("Enter tool[clearSnapshot, clearEvent, clearCvs] to run and appType[coinex, bitway] exceeded for clearSnapshot")
      sys.exit(0)
    }
    implicit val configFile: String = "clear.conf"
    PropertyConfigurator.configure(this.getClass.getClassLoader.getResource("tools.properties"))
    implicit val log = LoggerFactory.getLogger("HdfsFileClear")


    if ("clearSnapshot".equals(args(0)) && args.length >= 2) {
      new ClearSnapshot(args(1)).clear()
    } else if ("clearEvent".equals(args(0))) {
    } else if ("clearCvs".equals(args(0))) {
    } else {
      sys.error("Enter tool[clearSnapshot, clearEvent, clearCvs] to run and appType[coinex, bitway] exceeded for clearSnapshot")
    }

  }
}

trait ClearCommon {
  implicit val configFile: String
  protected val config = ConfigFactory.load(getClass.getClassLoader, configFile)
  protected val fs: FileSystem = openHdfsSystem(config.getString("clear.hdfsHost"))
  private def openHdfsSystem(defaultName: String): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.default.name", defaultName)
    FileSystem.get(conf)
  }
}

class ClearSnapshot(appType: String)(implicit val configFile: String, implicit val log: Logger) extends ClearCommon {
  private val snapshotHdfsDir = config.getString(s"clear.$appType.snapshot-dir")
  private val backupDir = config.getString(s"clear.$appType.backup-dir")
  private val namePattern = s"""snapshot~(.+)~([0-9]+)~([0-9]+)""".r
  val singletonSeqFiles = collection.mutable.Set.empty[String]
  val oneFile4OneDay = collection.mutable.Set.empty[String]

  val threeDaysBefore = java.util.Calendar.getInstance()
  threeDaysBefore.add(java.util.Calendar.DAY_OF_MONTH, -3)

  def clear() {
    log.info(">" * 50  + s"Begin ClearSnapshot for $appType")
    doClear(snapshotHdfsDir, backupDir)
    fs.close()
  }

  private def doClear(snapshotDir: String, backupPath: String) {
    val destDir = new File(backupPath)
    if (!destDir.exists()) {
      sys.error("Local backup directory not exists" + backupPath)
      sys.exit(-1)
    }
    val destFiles = destDir.listFiles().filter(_.isFile).map(_.getName).toSet
    fs.listStatus(new Path(snapshotDir)) foreach {
      file =>
        val fileName = file.getPath.getName
        fileName match {
          case namePattern(processorId, seqNumber, timestamp) =>
            val fileNameWithoutStamp = s"snapshot~$processorId~$seqNumber"
             if ("0".equals(seqNumber)) {
               fs.delete(file.getPath, false)
               log.info(s"delete zero seq file  -> $fileName")
             } else if (singletonSeqFiles.contains(fileNameWithoutStamp)) {
               log.info(s"delete duplicate file -> $fileName")
               fs.delete(file.getPath, false)
             } else {
               singletonSeqFiles += fileNameWithoutStamp
               // backup new files
               if (!destFiles.contains(fileName)) {
                 log.info(s"backup new file -> $fileName")
                 fs.copyToLocalFile(false, new Path(snapshotDir + "/" + fileName), new Path(backupPath))
//                 FileUtils.forceDeleteOnExit(new File(s"$backupPath/.$fileName.crc"))
               }
               // reduce files three days old, one day only keep one file
               val date = new java.util.Date(java.lang.Long.parseLong(timestamp))
               if (date.before(threeDaysBefore.getTime)) {
                 val dateStr = new java.text.SimpleDateFormat("yyyymmdd").format(date)
                 val oneDayFile = processorId + "~" + dateStr
                 if (oneFile4OneDay.contains(oneDayFile)) {
                   log.info(s"delete $dateStr redundance hdfs file -> $fileName")
                   fs.delete(file.getPath, false)
                 } else {
                   oneFile4OneDay += oneDayFile
                 }
               }
             }
        }
    }
  }
}