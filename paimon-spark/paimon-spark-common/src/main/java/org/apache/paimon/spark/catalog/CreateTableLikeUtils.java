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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.spark.SparkCatalogBase;
import org.apache.paimon.spark.SparkSource;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Shared logic for CREATE TABLE LIKE in Paimon Spark catalogs. */
public final class CreateTableLikeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTableLikeUtils.class);

    private static final String UNKNOWN_PROVIDER = "unknown";

    private static final String HIVE_STORED_AS = "hive.stored-as";
    private static final String HIVE_INPUT_FORMAT = "hive.input-format";
    private static final String HIVE_OUTPUT_FORMAT = "hive.output-format";
    private static final String HIVE_SERDE = "hive.serde";
    private static final String HIVE_SERDE_PROPERTY_PREFIX = "hive.serde.";

    private static final String STORED_AS_UNSUPPORTED_MESSAGE =
            "CREATE TABLE LIKE ... STORED AS is not supported for SparkCatalog.";

    private CreateTableLikeUtils() {}

    public static String resolveTargetProvider(
            Map<String, String> tableInfoProperties, Table sourceTable) {
        if (tableInfoProperties.containsKey(TableCatalog.PROP_PROVIDER)) {
            return tableInfoProperties.get(TableCatalog.PROP_PROVIDER);
        }
        return sourceTable.properties().get(TableCatalog.PROP_PROVIDER);
    }

    public static Table createTableLike(
            SparkCatalogBase catalog,
            Identifier ident,
            StructType schema,
            org.apache.spark.sql.connector.expressions.Transform[] partitions,
            Map<String, String> tableInfoProperties,
            Table sourceTable)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        validateNoHiveStorageSyntax(tableInfoProperties);

        Map<String, String> sourceProperties = sourceTable.properties();
        String sourceProvider =
                normalizedProvider(
                        sourceProperties.get(TableCatalog.PROP_PROVIDER), UNKNOWN_PROVIDER);
        String targetProvider =
                normalizedProvider(
                        tableInfoProperties.containsKey(TableCatalog.PROP_PROVIDER)
                                ? tableInfoProperties.get(TableCatalog.PROP_PROVIDER)
                                : sourceProperties.get(TableCatalog.PROP_PROVIDER),
                        SparkSource.NAME());

        Map<String, String> createProperties =
                buildCreateProperties(
                        sourceProperties, sourceProvider, targetProvider, tableInfoProperties);

        return catalog.createTable(ident, schema, partitions, createProperties);
    }

    private static void validateNoHiveStorageSyntax(Map<String, String> tableInfoProperties) {
        for (String key : tableInfoProperties.keySet()) {
            if (usesHiveStorageSyntax(key)) {
                throw new UnsupportedOperationException(STORED_AS_UNSUPPORTED_MESSAGE);
            }
        }
    }

    private static boolean usesHiveStorageSyntax(String propertyKey) {
        return HIVE_STORED_AS.equals(propertyKey)
                || HIVE_INPUT_FORMAT.equals(propertyKey)
                || HIVE_OUTPUT_FORMAT.equals(propertyKey)
                || HIVE_SERDE.equals(propertyKey)
                || propertyKey.startsWith(HIVE_SERDE_PROPERTY_PREFIX);
    }

    private static Map<String, String> buildCreateProperties(
            Map<String, String> sourceProperties,
            String sourceProvider,
            String targetProvider,
            Map<String, String> tableInfoProperties) {
        Map<String, String> properties = new HashMap<>();
        properties.putAll(copySourceProperties(sourceProperties, sourceProvider, targetProvider));
        properties.put(TableCatalog.PROP_PROVIDER, targetProvider);
        properties.putAll(tableInfoProperties);
        return properties;
    }

    private static Map<String, String> copySourceProperties(
            Map<String, String> sourceProperties, String sourceProvider, String targetProvider) {
        Map<String, String> copiedComment = new HashMap<>();
        if (sourceProperties.containsKey(TableCatalog.PROP_COMMENT)) {
            copiedComment.put(
                    TableCatalog.PROP_COMMENT, sourceProperties.get(TableCatalog.PROP_COMMENT));
        }

        Map<String, String> copiedProperties = filterCopiedProperties(sourceProperties);

        if (sourceProvider.equals(targetProvider)) {
            copiedProperties.putAll(copiedComment);
            return copiedProperties;
        }

        warnSkippedProperties(sourceProvider, targetProvider, copiedProperties.keySet());
        return copiedComment;
    }

    private static void warnSkippedProperties(
            String sourceProvider, String targetProvider, Iterable<String> skippedKeys) {
        StringBuilder builder = new StringBuilder();
        for (String key : skippedKeys) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(key);
        }
        if (builder.length() > 0) {
            LOG.warn(
                    "Skip copying source table properties in CREATE TABLE LIKE because source "
                            + "provider '{}' differs from target provider '{}': {}",
                    sourceProvider,
                    targetProvider,
                    builder);
        }
    }

    private static String normalizedProvider(String provider, String defaultProvider) {
        return provider == null ? defaultProvider : normalizeProvider(provider);
    }

    private static String normalizeProvider(String provider) {
        return provider.toLowerCase(Locale.ROOT);
    }

    private static Map<String, String> filterCopiedProperties(
            Map<String, String> sourceProperties) {
        Map<String, String> copied = new HashMap<>();
        for (Map.Entry<String, String> entry : sourceProperties.entrySet()) {
            String key = entry.getKey();
            if (CoreOptions.PATH.key().equals(key)) {
                continue;
            }
            if (TableCatalog.PROP_PROVIDER.equals(key)
                    || TableCatalog.PROP_COMMENT.equals(key)
                    || TableCatalog.PROP_LOCATION.equals(key)
                    || TableCatalog.PROP_OWNER.equals(key)
                    || TableCatalog.PROP_EXTERNAL.equals(key)
                    || TableCatalog.PROP_IS_MANAGED_LOCATION.equals(key)) {
                continue;
            }
            copied.put(key, entry.getValue());
        }
        return copied;
    }
}
