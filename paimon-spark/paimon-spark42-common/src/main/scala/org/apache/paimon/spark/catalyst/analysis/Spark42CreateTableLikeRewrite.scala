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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.{SparkCatalogBase, SparkGenericCatalogBase}
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.commands.PaimonCreateTableLikeCommand

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableLike, LogicalPlan, SerdeInfo}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

/** Rewrites Spark 4.2 V2 [[CreateTableLike]] for Paimon catalogs. */
case class Spark42CreateTableLikeRewrite(session: SparkSession) extends Rule[LogicalPlan] {

  private val storedAsUnsupportedMessage =
    "CREATE TABLE LIKE ... STORED AS is not supported for SparkCatalog."

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case create @ CreateTableLike(
          ResolvedIdentifier(targetCatalog, targetIdent),
          ResolvedTable(sourceCatalog, sourceIdent, _, _),
          location,
          provider,
          serdeInfo,
          properties,
          ifNotExists) =>
      if (usesHiveStorageSyntax(serdeInfo)) {
        throw new UnsupportedOperationException(storedAsUnsupportedMessage)
      }

      targetCatalog match {
        case catalog: SparkCatalogBase =>
          createTableLikeCommand(
            catalog,
            targetIdent,
            sourceCatalog,
            sourceIdent,
            provider,
            location,
            properties,
            ifNotExists)
        case catalog: SparkGenericCatalogBase if provider.exists(SparkBaseCatalog.usePaimon) =>
          createTableLikeCommand(
            catalog,
            targetIdent,
            sourceCatalog,
            sourceIdent,
            provider,
            location,
            properties,
            ifNotExists)
        case _ =>
          create
      }
    case other =>
      other
  }

  private def createTableLikeCommand(
      targetCatalog: TableCatalog,
      targetIdent: Identifier,
      sourceCatalog: TableCatalog,
      sourceIdent: Identifier,
      provider: Option[String],
      location: Option[String],
      properties: Map[String, String],
      ifNotExists: Boolean): PaimonCreateTableLikeCommand = {
    PaimonCreateTableLikeCommand(
      targetCatalog,
      targetIdent,
      sourceCatalog,
      sourceIdent,
      provider,
      location,
      properties,
      ifNotExists)
  }

  private def usesHiveStorageSyntax(serdeInfo: Option[SerdeInfo]): Boolean = {
    serdeInfo.exists(info => info.formatClasses.isDefined || info.storedAs.isDefined)
  }
}
