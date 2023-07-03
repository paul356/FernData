package org.ferndata.index.impl

import com.google.protobuf.ByteString

import java.net.URL
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Arrays
import java.util.UUID

import org.apache.iceberg.io.LocationProvider
import org.apache.iceberg.io.FileIO

import org.ferndata.index.DataLakeIndex
import org.ferndata.index.PersistedTypes.{IndexNodeType, KeyValuePair, NodeReference, PersistedIndexNode}

import scala.collection.immutable.List
import scala.collection.mutable.{Queue, TreeMap}
import scala.jdk.CollectionConverters._

private class WritableNode(
  dataBuffer: TreeMap[Array[Byte], (Byte, Array[Byte])],
  private var childRef: Option[TreeMap[(Array[Byte], Array[Byte]), URL]],
  nodeType: IndexNodeType) {

  private var dataSize = dataBuffer.foldLeft(0:Long)((curr, kv) => curr + kv._1.size + kv._2._2.size + 1);

  def put(key: Array[Byte], value: Array[Byte]) = {
    dataBuffer.put(key, (WritableNode.normalTag, value))
    dataSize += key.size + value.size + 1
  }

  def delete(key: Array[Byte]) = {
    if (childRef.isEmpty) {
      val oldVal = dataBuffer.remove(key)
      dataSize = dataSize - oldVal.fold(0)(_._2.size + 1)
    } else {
      dataBuffer.put(key, (WritableNode.deleteTag, Array.empty[Byte]))
      dataSize = dataSize + key.size + 1
    }
  }

  def putReference(ref: (Array[Byte], Array[Byte], URL)) = {
    childRef.get += (((ref._1, ref._2), ref._3))
  }

  private def serializeThisNode(locations: LocationProvider, io: FileIO) : (Array[Byte], Array[Byte], URL) = {
    val builder = PersistedIndexNode.newBuilder.setType(nodeType)
    dataBuffer.foreach(entry => {
      builder.addDatum(
        KeyValuePair.newBuilder
        .setKey(ByteString.copyFrom(entry._1))
        .setType(entry._2._1)
        .setValue(ByteString.copyFrom(entry._2._2))
        .build)
    })
    if (!childRef.isEmpty) {
      childRef.get.foreach(entry => {
        builder.addPointers(
          NodeReference.newBuilder
            .setMin(ByteString.copyFrom(entry._1._1))
            .setMax(ByteString.copyFrom(entry._1._2))
            .setUrl(entry._2.toString)
            .build)
      })
    }

    val fileType = nodeType match {
      case IndexNodeType.ROOT => "root"
      case IndexNodeType.INDIRECT => "indirect"
      case IndexNodeType.LEAF => "leaf"
      case IndexNodeType.UNRECOGNIZED => "unkown"
    }
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern(WritableNode.nameFormat))
    val fileUrl = locations.newDataLocation(s"$fileType-$timestamp-${UUID.randomUUID.toString}")
    val outputFile = io.newOutputFile(fileUrl)
    val outStream = outputFile.create

    val node = builder.build
    node.writeTo(outStream)
    outStream.flush
    outStream.close

    val minKey = if (childRef.isEmpty)
      dataBuffer.firstKey
    else if (dataBuffer.isEmpty)
      childRef.get.firstKey._1
    else if (WritableNode.ordering.compare(dataBuffer.firstKey, childRef.get.firstKey._1) <= 0)
      dataBuffer.firstKey
    else
      childRef.get.firstKey._1
    val maxKey = if (childRef.isEmpty)
      dataBuffer.lastKey
    else if (dataBuffer.isEmpty)
      childRef.get.lastKey._2
    else if (WritableNode.ordering.compare(dataBuffer.lastKey, childRef.get.lastKey._2) >= 0)
      dataBuffer.lastKey
    else
      childRef.get.lastKey._2

    return (minKey, maxKey, new URL(fileUrl))
  }


  def serialize(locations: LocationProvider, io: FileIO): List[(Array[Byte], Array[Byte], URL)] = {
    val isIndirectNode = !childRef.isEmpty
    if (isBufferOversized) {
      if (isIndirectNode) {
        val iter = childRef.get.iterator
        // childRef has at least one pointer
        val ((minKey, maxKey), url) = iter.next()
        var lastMinKey = (minKey, maxKey)
        var lastUrl = url
        val firstEntry = WritableNode(url, io)
        val foreachProc = (targetEntry: WritableNode, oldKey: (Array[Byte], Array[Byte])) => (kv: (Array[Byte], (Byte, Array[Byte]))) => {
          if (kv._2._1 == WritableNode.deleteTag) {
            targetEntry.delete(kv._1)
          } else {
            targetEntry.put(kv._1, kv._2._2)
          }
          childRef.get.remove(oldKey)
          val serializedUrl = targetEntry.serialize(locations, io)
          serializedUrl.foreach(putReference(_))
        }
        if (iter.hasNext) {
          val ((minKey, maxKey), url) = iter.next()
          // It is possible there are new KVs that are smaller than firstKey
          dataBuffer.rangeUntil(minKey).foreach(foreachProc(firstEntry, lastMinKey))
          lastMinKey = (minKey, maxKey)
          lastUrl = url
          while (iter.hasNext) {
            val ((minKey, maxKey), url) = iter.next()
            val targetEntry = WritableNode(lastUrl, io)
            dataBuffer.range(lastMinKey._1, minKey).foreach(foreachProc(targetEntry, lastMinKey))
            lastMinKey = (minKey, maxKey)
            lastUrl = url
          }
          // All left keys are pushed to the last child node
          val lastEntry = WritableNode(lastUrl, io)
          dataBuffer.rangeFrom(lastMinKey._1).foreach(foreachProc(lastEntry, lastMinKey))
        } else {
          // All entries is pushed to the only child node
          dataBuffer.foreach(foreachProc(firstEntry, lastMinKey))
        }
        dataBuffer.clear()

        // To split indirect pointers into different nodes
        if (isReferenceOversized) {
          childRef.get.grouped(WritableNode.maxRefNum).map(entry => {
            val node = WritableNode(entry, IndexNodeType.INDIRECT)
            node.serializeThisNode(locations, io)
          }).toList
        } else {
          List(serializeThisNode(locations, io))
        }
      } else {
        // ceiling when the size is larger than x.5
        val leafNum = (dataSize + (WritableNode.averageLeafSize >> 1)) / WritableNode.averageLeafSize
        val adjustSize = dataSize / leafNum
        val accuSize = dataBuffer.iterator.scanLeft((0:Long, Array.empty[Byte]))((sum, kv) => (sum._1 + kv._1.size + kv._2._2.size + 1, kv._1))
        val nodeIndex = accuSize.take(dataBuffer.size).map(entry => (entry._1 / adjustSize, entry._2))
        // Get the indices of changing value change.
        // For example nodeIndex is [0, 0, 0, 1, 1, 2, 2]
        // changePos is List[(-1, -1), (0, 0), (1, 3), (2, 5)]
        val changePos = nodeIndex.foldLeft(List((-1: Long, Array.empty[Byte])))((curr, kv) => {
          if (curr.last._1 != kv._1) {
            curr :+ ((kv._1, kv._2))
          } else {
            curr
          }
        })
        // Get the data ranges for partitions
        val ranges = changePos.zipAll(changePos.drop(1), (-1: Long, Array.empty[Byte]), (-1: Long, dataBuffer.lastKey.concat(Array[Byte](1)))).drop(1).map(kv => (kv._1._2, kv._2._2))
        ranges.foldLeft(List.empty[(Array[Byte], Array[Byte], URL)])((curr, entry) => {
          val node = new WritableNode(dataBuffer.range(entry._1, entry._2), Option.empty[TreeMap[(Array[Byte], Array[Byte]), URL]], IndexNodeType.LEAF)
          curr :+ node.serializeThisNode(locations, io)
        })
      }
    } else {
      List(serializeThisNode(locations, io))
    }
  }

  private def isBufferOversized: Boolean = {
    dataSize >= WritableNode.averageLeafSize * 2
  }

  private def isReferenceOversized: Boolean = {
    childRef.fold(0)(_.size) > WritableNode.maxRefNum
  }
}

private object WritableNode {
  val normalTag: Byte = 0
  val deleteTag: Byte = 1
  val averageLeafSize: Long = (64: Long) * 1024 * 1024
  val maxRefNum: Int = 1024
  val nameFormat = "YYYYMMddHHmmss"

  implicit val ordering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = {
      Arrays.compare(x, y)
    }
  }

  def apply(nodeAddr: URL, io: FileIO): WritableNode = {
    val inputFile = io.newInputFile(nodeAddr.toString)
    val inputStream = inputFile.newStream
    val persistedNode = PersistedIndexNode.parseFrom(inputStream)
    val writableNode = apply(persistedNode.getType)
    persistedNode.getDatumList.asScala.foreach(entry => {
      if (entry.getType != WritableNode.deleteTag)
        writableNode.put(entry.getKey.toByteArray, entry.getValue.toByteArray)
      else
        writableNode.delete(entry.getKey.toByteArray)
    })
    persistedNode.getPointersList.asScala.foreach(entry => writableNode.putReference((entry.getMin.toByteArray, entry.getMax.toByteArray, new URL(entry.getUrl))))
    writableNode
  }

  def apply(nodeType: IndexNodeType): WritableNode = {
    new WritableNode(TreeMap.empty[Array[Byte], (Byte, Array[Byte])], Option.empty[TreeMap[(Array[Byte], Array[Byte]), URL]], nodeType)
  }

  def apply(refs: TreeMap[(Array[Byte], Array[Byte]), URL], nodeType: IndexNodeType): WritableNode = {
    new WritableNode(TreeMap.empty[Array[Byte], (Byte, Array[Byte])], Some(refs), nodeType)
  }
}

private class ReadableNode(nodeAddr: URL) {

}

private class DataLakeIndexImpl(locations: LocationProvider, io: FileIO) extends DataLakeIndex {
  private val rootHistory = new Queue[(Array[Byte], Array[Byte], URL)];
  private var modifiedRoot = Option.empty[WritableNode]

  private def createWritableRoot: Unit = {
    if (modifiedRoot.isEmpty) {
      modifiedRoot = Some(if (rootHistory.isEmpty) WritableNode(IndexNodeType.ROOT) else WritableNode(rootHistory.last._3, io))
    }
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    createWritableRoot
    modifiedRoot.foreach(_.put(key, value))
  }
  def delete(key: Array[Byte]): Unit = {
    createWritableRoot
    modifiedRoot.foreach(_.delete(key))
  }
  def snapshot(txn: Long): Unit = {
    if (!modifiedRoot.isEmpty) {
      val result = modifiedRoot.map(_.serialize(locations, io))
      if (result.get.size > 1) {
        var refers = result.get;
        while (refers.size > 1) {
          val newRoot = WritableNode(IndexNodeType.ROOT)
          refers.foreach(newRoot.putReference(_))
          refers = newRoot.serialize(locations, io)
        }
        rootHistory.enqueue(refers.head)
      } else {
        rootHistory.enqueue(result.get.head)
      }
      modifiedRoot = Option.empty[WritableNode];
    }
  }
  def get(key: Array[Byte]): Option[Array[Byte]] = {
    Option.empty[Array[Byte]]
  }
  def minAfter(key: Array[Byte]): Option[(Array[Byte], Array[Byte])] = {
    Option.empty[(Array[Byte], Array[Byte])]
  }
  def maxBefore(key: Array[Byte]): Option[(Array[Byte], Array[Byte])] = {
    Option.empty[(Array[Byte], Array[Byte])]
  }
  def iterator(start: Array[Byte], stop: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] = {
    Option.empty[(Array[Byte], Array[Byte])].iterator
  }
}
