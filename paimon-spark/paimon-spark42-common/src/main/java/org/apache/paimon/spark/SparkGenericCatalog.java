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

package org.apache.paimon.spark;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableInfo;

import java.util.Map;

/** A Spark catalog that can also load non-Paimon tables. */
public class SparkGenericCatalog extends SparkGenericCatalogBase {

    @Override
    public Table createTableLike(Identifier ident, TableInfo tableInfo, Table sourceTable)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        // Match V1 SparkGenericCatalog routing: only use the inner Paimon catalog when the
        // target table explicitly specifies a Paimon provider (USING paimon). Do not infer
        // routing from the source table provider.
        Map<String, String> properties = tableInfo.properties();
        if (properties.containsKey(TableCatalog.PROP_PROVIDER)
                && usePaimon(properties.get(TableCatalog.PROP_PROVIDER))) {
            return ((TableCatalog) getSparkCatalog())
                    .createTableLike(ident, tableInfo, sourceTable);
        } else {
            return asSessionTableCatalog().createTableLike(ident, tableInfo, sourceTable);
        }
    }
}
