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

package org.apache.spark.sql.paimon.shims

import org.apache.paimon.data.variant.Variant
import org.apache.paimon.spark.data.{SparkArrayData, SparkInternalRow}
import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.table.{FileStoreTable, FormatTable}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, UnresolvedFunction}
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, CTERelationRef, DescribeRelation, InsertAction, LogicalPlan, MergeAction, MergeIntoTable, OverwriteByExpression, OverwritePartitionsDynamic, SubqueryAlias, TableSpec, UnresolvedWith, UpdateAction}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Column, Identifier, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * A spark shim trait. It declares methods which have incompatible implementations between Spark 3
 * and Spark 4. The specific SparkShim implementation will be loaded through Service Provider
 * Interface.
 */
trait SparkShim {

  def classicApi: ClassicApi

  def createSparkParser(delegate: ParserInterface): ParserInterface

  def createCustomResolution(spark: SparkSession): Rule[LogicalPlan]

  def createSparkInternalRow(rowType: RowType): SparkInternalRow

  def createSparkInternalRowWithBlob(
      rowType: RowType,
      blobFields: Set[Int],
      blobAsDescriptor: Boolean): SparkInternalRow

  def createSparkArrayData(elementType: DataType): SparkArrayData

  def createTable(
      tableCatalog: TableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table

  def createReplaceTableAsSelectExec(
      catalog: TableCatalog,
      ident: Identifier,
      partitioning: Seq[Transform],
      query: LogicalPlan,
      tableSpec: TableSpec,
      writeOptions: Map[String, String],
      orCreate: Boolean): SparkPlan

  def createAtomicReplaceTableAsSelectExec(
      catalog: StagingTableCatalog,
      ident: Identifier,
      partitioning: Seq[Transform],
      query: LogicalPlan,
      tableSpec: TableSpec,
      writeOptions: Map[String, String],
      orCreate: Boolean): SparkPlan

  def createReplaceTableExec(
      catalog: TableCatalog,
      ident: Identifier,
      columns: Array[Column],
      partitioning: Seq[Transform],
      tableSpec: TableSpec,
      orCreate: Boolean): SparkPlan

  def createAtomicReplaceTableExec(
      catalog: StagingTableCatalog,
      ident: Identifier,
      columns: Array[Column],
      partitioning: Seq[Transform],
      tableSpec: TableSpec,
      orCreate: Boolean): SparkPlan

  def toReplaceTableColumns(
      tableSchema: StructType,
      schemaOrColumns: Any,
      catalog: TableCatalog,
      ident: Identifier): Array[Column]

  def copyTableSpec(
      tableSpec: TableSpec,
      additionalProperties: Map[String, String],
      location: Option[String]): TableSpec

  /**
   * Constructs a `BatchWrite` for Paimon's V2 write path. The implementation lives in each
   * per-version shim module so the `extends BatchWrite` mixin is compiled against the right Spark
   * minor version: Spark 4.1 added a default method `BatchWrite.commit(.., WriteSummary)` whose
   * inherited signature triggers `ClassNotFoundException: WriteSummary` lazy-linking on Spark 4.0
   * runtimes when the class is loaded for task serialization.
   */
  def createPaimonBatchWrite(
      table: FileStoreTable,
      writeSchema: StructType,
      dataSchema: StructType,
      overwritePartitions: Option[Map[String, String]],
      copyOnWriteScan: Option[PaimonCopyOnWriteScan]): BatchWrite

  /** Same `BatchWrite` mixin problem as [[createPaimonBatchWrite]], but for `FormatTable` writes. */
  def createFormatTableBatchWrite(
      table: FormatTable,
      overwriteDynamic: Option[Boolean],
      overwritePartitions: Option[Map[String, String]],
      writeSchema: StructType): BatchWrite

  def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean): CTERelationRef

  def supportsHashAggregate(
      aggregateBufferAttributes: Seq[Attribute],
      groupingExpression: Seq[Expression]): Boolean

  def supportsObjectHashAggregate(
      aggregateExpressions: Seq[AggregateExpression],
      groupByExpressions: Seq[Expression]): Boolean

  def createMergeIntoTable(
      targetTable: LogicalPlan,
      sourceTable: LogicalPlan,
      mergeCondition: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction],
      withSchemaEvolution: Boolean): MergeIntoTable

  /**
   * Constructs the Paimon fallback command for dynamic partition overwrite. Spark 4.2 extended
   * [[V2WriteCommand]] with
   * [[org.apache.spark.sql.catalyst.plans.logical.WriteWithSchemaEvolution]], so the concrete
   * command class is version-specific and must be built behind this shim.
   */
  def createPaimonDynamicPartitionOverwriteCommand(
      table: NamedRelation,
      fileStoreTable: FileStoreTable,
      query: LogicalPlan,
      writeOptions: Map[String, String],
      isByName: Boolean,
      source: OverwritePartitionsDynamic): LogicalPlan

  /**
   * Spark 4.2 added `withSchemaEvolution` to [[OverwriteByExpression]] factory methods. The
   * signature must be constructed behind this shim.
   */
  def createOverwriteByExpressionByName(
      table: NamedRelation,
      query: LogicalPlan,
      condition: Expression,
      writeOptions: Map[String, String],
      withSchemaEvolution: Boolean = false): LogicalPlan

  /**
   * Spark 4.2 added `withSchemaEvolution` to [[OverwritePartitionsDynamic]] factory methods. The
   * signature must be constructed behind this shim.
   */
  def createOverwritePartitionsDynamicByName(
      table: NamedRelation,
      query: LogicalPlan,
      writeOptions: Map[String, String],
      withSchemaEvolution: Boolean = false): LogicalPlan

  /**
   * Returns the partition spec for [[DescribeRelation]]. Spark 4.2 removed `partitionSpec` from
   * `DescribeRelation` (partition describe moved to [[DescribeTablePartition]]), so this accessor
   * must be version-specific to avoid `NoSuchMethodError` at runtime.
   */
  def describeRelationPartitionSpec(describe: DescribeRelation): Map[String, String]

  /**
   * Plans Spark 4.2 [[DescribeTablePartition]] for Paimon tables. Returns `None` on Spark versions
   * that do not have this logical node.
   */
  def planDescribeTablePartition(spark: SparkSession, plan: LogicalPlan): Option[Seq[SparkPlan]]

  /**
   * Creates an empty [[CatalogStorageFormat]] for partition describe output. Spark 4.2 added a
   * `serdeName` field to [[CatalogStorageFormat]], so the constructor signature must be built
   * behind this shim to avoid `NoSuchMethodError` at runtime.
   */
  def createEmptyCatalogStorageFormat(): CatalogStorageFormat

  // Spark 3.4 added `notMatchedBySourceActions` to `MergeIntoTable`. On 3.2/3.3 the field doesn't
  // exist on the AST, so this returns `Seq.empty`. Lets `paimon-spark-common` (which compiles
  // against 3.5/4.1) reference NMBS via a single accessor that works on all minor versions.
  def notMatchedBySourceActions(merge: MergeIntoTable): Seq[MergeAction]

  // Per-version shim: Spark 4.1 added a 3rd `fromStar: Boolean = false` field. A 2-arg call site
  // compiled against 4.1 emits an `apply$default$3()` lookup absent on 4.0. Paimon tracks star
  // intent via [[PaimonMergeActionTags]], so `fromStar` stays unused here.
  def createUpdateAction(condition: Option[Expression], assignments: Seq[Assignment]): UpdateAction

  def createInsertAction(condition: Option[Expression], assignments: Seq[Assignment]): InsertAction

  def copyDataSourceV2Relation(
      relation: DataSourceV2Relation,
      table: Table,
      output: Seq[org.apache.spark.sql.catalyst.expressions.AttributeReference])
      : DataSourceV2Relation

  /**
   * Returns the list of "early" substitution rules Paimon needs to apply on a parsed view plan.
   * Spark 3.x exposes both `CTESubstitution` and `SubstituteUnresolvedOrdinals`, but 4.1 removed
   * `SubstituteUnresolvedOrdinals` (its work is handled by the new resolver framework), so the
   * concrete shim chooses the appropriate set for the active Spark version.
   */
  def earlyBatchRules(): Seq[Rule[LogicalPlan]]

  // Build a `MergeRows.Keep` instruction for Paimon's merge rewrites. Spark 4.1 added a leading
  // `Context` parameter; Spark < 3.4 does not have `MergeRows` at all. Returning `AnyRef` here
  // keeps the trait signature free of `MergeRows` so Spark3Shim can link on Spark 3.2 / 3.3.
  def mergeRowsKeepCopy(condition: Expression, output: Seq[Expression]): AnyRef

  def mergeRowsKeepUpdate(condition: Expression, output: Seq[Expression]): AnyRef

  def mergeRowsKeepInsert(condition: Expression, output: Seq[Expression]): AnyRef

  /**
   * Returns a new `UnresolvedWith` with each CTE's `SubqueryAlias` rewritten by the given function.
   * Spark 4.1 extended the cteRelations element tuple from `(String, SubqueryAlias)` to
   * `(String, SubqueryAlias, Option[Int])`, so rebuilding the tuple must live behind a shim.
   */
  def transformUnresolvedWithCteRelations(
      u: UnresolvedWith,
      transform: SubqueryAlias => SubqueryAlias): UnresolvedWith

  /**
   * Returns true when the given set of paths points at a file-stream sink metadata location
   * (formerly `FileStreamSink.hasMetadata`). Spark 4.1 relocated `FileStreamSink` from
   * `org.apache.spark.sql.execution.streaming` to `...streaming.sinks`, so the call must be
   * shimmed.
   */
  def hasFileStreamSinkMetadata(
      paths: Seq[String],
      hadoopConf: org.apache.hadoop.conf.Configuration,
      sqlConf: org.apache.spark.sql.internal.SQLConf): Boolean

  /**
   * Creates a `PartitioningAwareFileIndex` backed by a streaming `MetadataLogFileIndex` with an
   * overridden `partitionSchema`. Spark 4.1 relocated `MetadataLogFileIndex` from
   * `...streaming.MetadataLogFileIndex` to `...streaming.runtime.MetadataLogFileIndex`, so the
   * Paimon subclass lives in each version-specific shim module.
   */
  def createPartitionedMetadataLogFileIndex(
      sparkSession: SparkSession,
      path: org.apache.hadoop.fs.Path,
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType)
      : org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex

  // for variant
  def toPaimonVariant(o: Object): Variant

  def toPaimonVariant(row: InternalRow, pos: Int): Variant

  def toPaimonVariant(array: ArrayData, pos: Int): Variant

  def isSparkVariantType(dataType: org.apache.spark.sql.types.DataType): Boolean

  def SparkVariantType(): org.apache.spark.sql.types.DataType

  /**
   * Creates a Spark `CharType`. Spark 4.2 (SPARK-54870) replaced the single-arg `CharType(length)`
   * constructor with a collation-aware case class, so the 4.2 shim must use `CharType.apply`.
   */
  def createCharType(length: Int): org.apache.spark.sql.types.DataType

  /** Same collation constructor change as [[createCharType]] for `VarcharType`. */
  def createVarcharType(length: Int): org.apache.spark.sql.types.DataType

  /**
   * Qualifies a persistent V1 function identifier before registration in
   * [[org.apache.spark.sql.catalyst.catalog.PaimonV1FunctionRegistry]]. Spark 4.2 requires 3-part
   * identifiers (catalog.database.function) in
   * [[org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry]].
   */
  def qualifyV1FunctionIdentifier(
      session: SparkSession,
      ident: FunctionIdentifier): FunctionIdentifier

  /**
   * Reads the IGNORE NULLS flag from an unresolved function. Spark 4.2 changed
   * [[UnresolvedFunction.ignoreNulls]] from `Boolean` to `Option[Boolean]`.
   */
  def unresolvedFunctionIgnoreNulls(u: UnresolvedFunction): Boolean
}
