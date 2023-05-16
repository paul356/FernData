package org.apache.iceberg.spark.source

import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.SparkWriteConf
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.Table
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.SparkSession

class FernDataWriteBuilder(
  val spark: SparkSession,
  val table: Table,
  val branch: String,
  val info: LogicalWriteInfo) extends WriteBuilder {
  // members
  val writeConf = new SparkWriteConf(spark, table, branch, info.options())
  val dsSchema = info.schema()

  override def build: Write = {
    val writeSchema = SparkSchemaUtil.convert(table.schema, dsSchema, writeConf.caseSensitive)
    TypeUtil.validateWriteSchema(table.schema, writeSchema, writeConf.checkNullability, writeConf.checkOrdering)
    return new FernDataWrite(spark, table, writeConf.dataFileFormat, writeSchema, dsSchema)
  }
}
