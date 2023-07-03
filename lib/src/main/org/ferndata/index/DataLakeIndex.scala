package org.ferndata.index

trait DataLakeIndex {
  def put(key : Array[Byte], value : Array[Byte]): Unit;
  def get(key : Array[Byte]) : Option[Array[Byte]];
  def delete(key: Array[Byte]): Unit;
  def minAfter(key : Array[Byte]) : Option[(Array[Byte], Array[Byte])];
  def maxBefore(key : Array[Byte]) : Option[(Array[Byte], Array[Byte])];
  def iterator(start : Array[Byte], stop : Array[Byte]) : Iterator[(Array[Byte], Array[Byte])];
}
