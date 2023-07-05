package org.ferndata.index

import org.apache.iceberg.io.LocationProvider
import org.apache.iceberg.io.FileIO

import org.ferndata.index.impl.DataLakeIndexImpl

trait DataLakeIndex {
  def put(key : Array[Byte], value : Array[Byte]): Unit;
  def get(key : Array[Byte]) : Option[Array[Byte]];
  def delete(key: Array[Byte]): Unit;
  def snapshot(txn: Long): Unit;
  def minAfter(key : Array[Byte]) : Option[(Array[Byte], Array[Byte])];
  def maxBefore(key : Array[Byte]) : Option[(Array[Byte], Array[Byte])];
  def iterator(start : Array[Byte], stop : Array[Byte]) : Iterator[(Array[Byte], Array[Byte])];
}

object DataLakeIndex {
  def apply(locations: LocationProvider, io: FileIO): DataLakeIndex = {
    new DataLakeIndexImpl(locations, io)
  }
}
