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

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.catalyst.analysis.PaimonResolvePartitionSpec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{DescribeTablePartition, LogicalPlan}
import org.apache.spark.sql.execution.{PaimonDescribeTableExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

object Spark42DescribeTablePlanning {

  def plan(spark: SparkSession, plan: LogicalPlan): Option[Seq[SparkPlan]] = plan match {
    case d: DescribeTablePartition =>
      d.table match {
        case r: ResolvedTable =>
          (r.table, r.catalog) match {
            case (sparkTable: SparkTable, sparkCatalog: SparkBaseCatalog) =>
              val partitionSchema = r.table.asPartitionable.partitionSchema()
              val resolved =
                PaimonResolvePartitionSpec.resolve(r.catalog, r.identifier, d.partitionSpec)
              val partitionSpec =
                PaimonResolvePartitionSpec.toTablePartitionSpec(partitionSchema, resolved)
              Some(
                PaimonDescribeTableExec(
                  d.output,
                  sparkCatalog,
                  r.identifier,
                  sparkTable,
                  partitionSpec,
                  d.isExtended) :: Nil)
            case _ => None
          }
        case _ => None
      }
    case _ => None
  }
}
