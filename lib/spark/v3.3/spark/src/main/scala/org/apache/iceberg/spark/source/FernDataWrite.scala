package org.apache.iceberg.spark.source

import java.io.FileNotFoundException
import java.io.IOException
import java.util.UUID
import java.util.function.Consumer

import org.apache.iceberg.encryption.EncryptedOutputFile
import org.apache.iceberg.encryption.EncryptionManager

import org.apache.iceberg.DataFile
import org.apache.iceberg.FileFormat
import org.apache.iceberg.io.DataWriteResult
import org.apache.iceberg.io.FileInfo
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.io.LocationProvider
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.io.SupportsPrefixOperations
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.connector.write.DataWriterFactory
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.immutable.StringOps

class FernDataWrite(
  spark: SparkSession,
  table: Table,
  fileFormat: FileFormat,
  schema: Schema,
  dsSchema: StructType) extends Write {
  val logFileListPrefix = "log-file-list.";

  override def toBatch: BatchWrite = {
    return new BatchAppend
  }

  private class BatchAppend extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      val tableBroadcast = spark.sparkContext.broadcast(SerializableTableWithSize.copyOf(table))
      return new FernDataWriterFactory(tableBroadcast, fileFormat, schema, dsSchema)
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
    }

    override def toString: String = {
      return "A FernData BatchWrite"
    }

    private def getMaxFileId(prefixOps: SupportsPrefixOperations, dir: String, prefix: String): Long = {
      var maxSuffixId:Int = 0

      try {
        val files = prefixOps.listPrefix(dir)

        val findMaxId = new Consumer[FileInfo]() {
          override def accept(fi: FileInfo) = {
            println(s"has file ${fi.location} in $dir")
            val fullPrefix = s"file:${dir}/${prefix}"
            val fl: String = fi.location
            if (fl.startsWith(fullPrefix)) {
              val suffix = fl.substring(fullPrefix.length)
              val suffixId = suffix.toInt
              if (suffixId > maxSuffixId) {
                maxSuffixId = suffixId
              }
            }
          }
        }
        files.forEach(findMaxId)
      } catch {
        case cause: FileNotFoundException => return 0
        case cause: Throwable => {
          println(s"exception is $cause")
          throw cause
        }
      }

      return maxSuffixId
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      val locationProvider = table.locationProvider
      val fileIO = table.io

      if (!fileIO.isInstanceOf[SupportsPrefixOperations]) {
        throw new IOException("requires prefix support")
      }

      val prefixOps = fileIO.asInstanceOf[SupportsPrefixOperations]
      val tableOps = table.asInstanceOf[HasTableOperations].operations

      val metaPath = tableOps.metadataFileLocation(".")

      val nextSuffixId = getMaxFileId(prefixOps, metaPath, logFileListPrefix) + 1
      val newFileName = s"$metaPath/$logFileListPrefix$nextSuffixId"

      val fileListFile = fileIO.newOutputFile(newFileName)
      println(s"going to create $newFileName")
      val outputStream = fileListFile.create
      messages.foreach(ws => {
        val files = ws.asInstanceOf[TaskCommit].files
        files.foreach(file => {
          outputStream.write(file.path.toString.getBytes)
          outputStream.write("\n".getBytes)
        })
      })
      outputStream.flush
      outputStream.close
    }
  }
}
 
private class FernDataFileFactory(
  format: FileFormat,
  locations: LocationProvider,
  io: FileIO,
  encryptionManager: EncryptionManager,
  partitionId: Int,
  taskId: Long,
  operationId: String
) {

  private def generateFilename: String = {
    return format.addExtension(s"first-file-$partitionId-$taskId-$operationId")
  }

  def newOutputFile: EncryptedOutputFile = {
    val file = io.newOutputFile(locations.newDataLocation(generateFilename))
    return encryptionManager.encrypt(file)
  }
}

private class TaskCommit(val files: Array[DataFile]) extends WriterCommitMessage {
}

private class DataLogDataWriter(
  writerFactory: SparkFileWriterFactory,
  fileFactory: FernDataFileFactory,
  io: FileIO,
  spec: PartitionSpec) extends DataWriter[InternalRow] {
  val writer: org.apache.iceberg.io.DataWriter[InternalRow] = writerFactory.newDataWriter(fileFactory.newOutputFile, spec, null)

  override def write(record: InternalRow): Unit = {
    writer.write(record)
  }

  override def commit: WriterCommitMessage = {
    close();

    val result = new DataWriteResult(writer.toDataFile)
    val taskCommit = new TaskCommit(result.dataFiles.toArray(new Array[DataFile](0)));
    return taskCommit
  }

  override def abort: Unit = {

  }

  override def close: Unit = {
    writer.close
  }
}

private class FernDataWriterFactory(
  tableBroadcast: Broadcast[Table],
  fileFormat: FileFormat,
  writeSchema: Schema,
  dsSchema: StructType) extends DataWriterFactory {
  def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val table = tableBroadcast.value

    val fileFactory = new FernDataFileFactory(
      fileFormat,
      table.locationProvider,
      table.io,
      table.encryption,
      partitionId,
      taskId,
      UUID.randomUUID.toString)

    val writerFactory = SparkFileWriterFactory.builderFor(table)
      .dataFileFormat(fileFormat)
      .dataSchema(writeSchema)
      .dataSparkType(dsSchema)
      .build

    return new DataLogDataWriter(writerFactory, fileFactory, table.io, table.spec)
  }
}
