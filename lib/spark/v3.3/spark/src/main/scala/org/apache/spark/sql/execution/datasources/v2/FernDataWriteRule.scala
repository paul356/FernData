package org.apache.spark.sql.execution.datasources.v2

import java.util.UUID
import org.apache.iceberg.spark.source.FernDataWriteBuilder
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.ExtendedLogicalWriteInfoImpl
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object FernDataWriteRule extends Rule[LogicalPlan] {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, None) if isIcebergRelation(r) =>
      val writeBuilder = newWriteBuilder(r.table, query.schema, options)
      val write = writeBuilder.build()
      val newQuery = ExtendedDistributionAndOrderingUtils.prepareQuery(write, query, conf)
      a.copy(write = Some(write), query = newQuery)
  }

  private def newWriteBuilder(
      table: Table,
      rowSchema: StructType,
      writeOptions: Map[String, String],
      rowIdSchema: StructType = null,
      metadataSchema: StructType = null): WriteBuilder = {
    val info = ExtendedLogicalWriteInfoImpl(
      queryId = UUID.randomUUID().toString,
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema,
      metadataSchema)
    new FernDataWriteBuilder(SparkSession.active, table.asInstanceOf[SparkTable].table, "", info);
  }
}
