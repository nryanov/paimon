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

package org.apache.paimon.spark.commands

import org.apache.paimon.options.Options
import org.apache.paimon.spark.DynamicOverWrite
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.{createDataset, createNewDataFrame}
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.connector.catalog.TableWritePrivilege
import org.apache.spark.sql.execution.command.RunnableCommand

import scala.collection.convert.ImplicitConversions._

/**
 * Spark 4.2 variant of [[PaimonDynamicPartitionOverwriteCommand]] that implements
 * [[org.apache.spark.sql.catalyst.plans.logical.WriteWithSchemaEvolution]] via [[V2WriteCommand]].
 */
case class PaimonSpark42DynamicPartitionOverwriteCommand(
    table: NamedRelation,
    fileStoreTable: FileStoreTable,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    withSchemaEvolution: Boolean)
  extends RunnableCommand
  with V2WriteCommand {

  override def writePrivileges: Set[TableWritePrivilege] =
    Set(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)

  override def child: LogicalPlan = query

  override def withNewQuery(
      newQuery: LogicalPlan): PaimonSpark42DynamicPartitionOverwriteCommand = {
    copy(query = newQuery)
  }

  override def withNewTable(
      newTable: NamedRelation): PaimonSpark42DynamicPartitionOverwriteCommand = {
    copy(table = newTable)
  }

  override protected def withNewChildInternal(
      newChild: LogicalPlan): PaimonSpark42DynamicPartitionOverwriteCommand = copy(query = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    WriteIntoPaimonTable(
      fileStoreTable,
      DynamicOverWrite,
      createNewDataFrame(createDataset(sparkSession, query)),
      Options.fromMap(fileStoreTable.options() ++ writeOptions)
    ).run(sparkSession)
  }

  override def storeAnalyzedQuery(): Command = copy(query = query)
}
