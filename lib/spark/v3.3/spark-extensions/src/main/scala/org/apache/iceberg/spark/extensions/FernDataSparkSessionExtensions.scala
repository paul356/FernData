package org.apache.iceberg.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.analysis.AlignedRowLevelIcebergCommandCheck
import org.apache.spark.sql.catalyst.analysis.AlignRowLevelCommandAssignments
import org.apache.spark.sql.catalyst.analysis.CheckMergeIntoTableConditions
import org.apache.spark.sql.catalyst.analysis.MergeIntoIcebergTableResolutionCheck
import org.apache.spark.sql.catalyst.analysis.ProcedureArgumentCoercion
import org.apache.spark.sql.catalyst.analysis.ResolveMergeIntoTableReferences
import org.apache.spark.sql.catalyst.analysis.ResolveProcedures
import org.apache.spark.sql.catalyst.analysis.RewriteDeleteFromIcebergTable
import org.apache.spark.sql.catalyst.analysis.RewriteMergeIntoTable
import org.apache.spark.sql.catalyst.analysis.RewriteUpdateTable
import org.apache.spark.sql.catalyst.optimizer.ExtendedReplaceNullWithFalseInPredicate
import org.apache.spark.sql.catalyst.optimizer.ExtendedSimplifyConditionalsInPredicate
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy
import org.apache.spark.sql.execution.datasources.v2.FernDataWriteRule
import org.apache.spark.sql.execution.datasources.v2.OptimizeMetadataOnlyDeleteFromIcebergTable
import org.apache.spark.sql.execution.datasources.v2.ReplaceRewrittenRowLevelCommand
import org.apache.spark.sql.execution.datasources.v2.RowLevelCommandScanRelationPushDown
import org.apache.spark.sql.execution.dynamicpruning.RowLevelCommandDynamicPruning

class FernDataSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }

    // analyzer extensions
    extensions.injectResolutionRule { spark => ResolveProcedures(spark) }
    extensions.injectResolutionRule { spark => ResolveMergeIntoTableReferences(spark) }
    extensions.injectResolutionRule { _ => CheckMergeIntoTableConditions }
    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }
    extensions.injectResolutionRule { _ => AlignRowLevelCommandAssignments }
    extensions.injectResolutionRule { _ => RewriteDeleteFromIcebergTable }
    extensions.injectResolutionRule { _ => RewriteUpdateTable }
    extensions.injectResolutionRule { _ => RewriteMergeIntoTable }
    extensions.injectCheckRule { _ => MergeIntoIcebergTableResolutionCheck }
    extensions.injectCheckRule { _ => AlignedRowLevelIcebergCommandCheck }

    // optimizer extensions
    extensions.injectOptimizerRule { _ => ExtendedSimplifyConditionalsInPredicate }
    extensions.injectOptimizerRule { _ => ExtendedReplaceNullWithFalseInPredicate }
    // pre-CBO rules run only once and the order of the rules is important
    // - metadata deletes have to be attempted immediately after the operator optimization
    // - dynamic filters should be added before replacing commands with rewrite plans
    // - scans must be planned before building writes
    extensions.injectPreCBORule { _ => OptimizeMetadataOnlyDeleteFromIcebergTable }
    extensions.injectPreCBORule { _ => RowLevelCommandScanRelationPushDown }
    extensions.injectPreCBORule { _ => FernDataWriteRule }
    extensions.injectPreCBORule { spark => RowLevelCommandDynamicPruning(spark) }
    extensions.injectPreCBORule { _ => ReplaceRewrittenRowLevelCommand }

    // planner extensions
    extensions.injectPlannerStrategy { spark => ExtendedDataSourceV2Strategy(spark) }
  }
}
