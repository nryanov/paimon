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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.optimizer.OptimizeMetadataOnlyDeleteFromPaimonTable
import org.apache.paimon.spark.commands.DeleteFromPaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan, ReplaceData}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Spark 4.2+ shadow of Spark41DeleteMetadataRestore.
 *
 * Kept under the same FQN as the spark4-common implementation so paimon-spark-4.2 can shadow it at
 * runtime and avoid binary incompatibilities when Spark's RewriteRowLevelCommand changes.
 */
object Spark41DeleteMetadataRestore extends RewriteRowLevelCommand with PureAppendOnlyScope {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case rd: ReplaceData if isMetadataOnlyDeleteOnAppendOnlyPaimon(rd) =>
          val origRelation = rd.originalTable.asInstanceOf[DataSourceV2Relation]
          val fs = origRelation.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
          DeleteFromPaimonTableCommand(origRelation, fs, rd.condition)
      }
    }
  }

  private def isMetadataOnlyDeleteOnAppendOnlyPaimon(rd: ReplaceData): Boolean = {
    val writeIsDelete = rd.table match {
      case r: DataSourceV2Relation =>
        r.table match {
          case op: RowLevelOperationTable => op.operation.command() == DELETE
          case _ => false
        }
      case _ => false
    }
    writeIsDelete && (rd.originalTable match {
      case r: DataSourceV2Relation if targetsV2CopyOnWriteTable(r) =>
        r.table match {
          case spk: SparkTable =>
            spk.getTable match {
              case fs: FileStoreTable =>
                OptimizeMetadataOnlyDeleteFromPaimonTable.isMetadataOnlyDelete(fs, rd.condition)
              case _ => false
            }
          case _ => false
        }
      case _ => false
    })
  }
}
