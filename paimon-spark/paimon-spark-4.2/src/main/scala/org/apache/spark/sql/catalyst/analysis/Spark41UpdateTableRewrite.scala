/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.paimon.spark.catalyst.analysis.PaimonAssignmentUtils

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualNullSafe, Expression, If, Literal, MetadataAttribute, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, Assignment, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateTable}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{COPY_OPERATION, OPERATION_COLUMN, UPDATE_OPERATION}
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Spark 4.2+ shadow of Spark41UpdateTableRewrite.
 *
 * Kept under the same FQN as the spark4-common implementation so paimon-spark-4.2 can shadow it at
 * runtime and avoid binary incompatibilities when Spark's RewriteRowLevelCommand changes.
 */
object Spark41UpdateTableRewrite extends RewriteRowLevelCommand with PureAppendOnlyScope {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case u @ UpdateTable(aliasedTable, assignments, cond)
            if u.resolved && u.rewritable && targetsV2CopyOnWriteTable(aliasedTable) =>
          EliminateSubqueryAliases(aliasedTable) match {
            case r @ ExtractV2Table(tbl: SupportsRowLevelOperations) =>
              val table = buildOperationTable(tbl, UPDATE, CaseInsensitiveStringMap.empty())
              val updateCond = cond.getOrElse(TrueLiteral)
              // `ResolveAssignments` fires later in the batch, so `u.aligned` is still false.
              // Pre-align via the same utility the postHoc V1 fallback uses.
              val alignedAssignments = PaimonAssignmentUtils.alignUpdateAssignments(
                r.output,
                assignments,
                fromStar = false,
                mergeSchemaEnabled = false)
              if (SubqueryExpression.hasSubquery(updateCond)) {
                buildReplaceDataWithUnionPlan(r, table, alignedAssignments, updateCond)
              } else {
                buildReplaceDataPlan(r, table, alignedAssignments, updateCond)
              }
            case _ =>
              u
          }
      }
    }
  }

  // Mirrors Spark 4.2 RewriteUpdateTable group-based paths while preserving Paimon-specific
  // pre-alignment and Resolution-batch interception semantics.
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    val query = buildReplaceDataUpdateProjection(readRelation, assignments, cond)
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
  }

  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildReplaceDataUpdateProjection(matchedRowsPlan, assignments)

    val remainingRowFilter = Not(EqualNullSafe(cond, TrueLiteral))
    val remainingRowsPlan =
      addOperationColumn(COPY_OPERATION, Filter(remainingRowFilter, readRelation))

    val query = Union(updatedRowsPlan, remainingRowsPlan)
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(query, relation.output, metadataAttrs)
    val groupFilterCond = if (groupFilterEnabled) Some(cond) else None
    ReplaceData(writeRelation, cond, query, relation, projections, groupFilterCond)
  }

  /** Assumes assignments are already aligned with the table output. */
  private def buildReplaceDataUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = TrueLiteral): LogicalPlan = {
    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map {
      case (attr, index) =>
        if (index < assignments.size) {
          val assignedExpr = assignedValues(index)
          val updatedValue = If(cond, assignedExpr, attr)
          Alias(updatedValue, attr.name)()
        } else {
          assert(MetadataAttribute.isValid(attr.metadata))
          if (MetadataAttribute.isPreservedOnUpdate(attr)) {
            attr
          } else {
            val updatedValue = If(cond, Literal(null, attr.dataType), attr)
            Alias(updatedValue, attr.name)(explicitMetadata = Some(attr.metadata))
          }
        }
    }
    val operationExpr = If(cond, Literal(UPDATE_OPERATION), Literal(COPY_OPERATION))
    val operationCol = Alias(operationExpr, OPERATION_COLUMN)()
    Project(operationCol +: updatedValues, plan)
  }
}
