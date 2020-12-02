/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Properties;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

/**
 * Configuration for Spark sources.
 */
@SourceType(value = "SPARK", label = "Databricks Spark", uiConfig = "spark-layout.json")
public class SparkConf extends AbstractArpConf<SparkConf> {
  private static final String ARP_FILENAME = "arp/implementation/spark-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "com.simba.spark.jdbc.Driver";

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Server Hostname")
  public String hostname;

  @NotBlank
  @Tag(2)
  @Min(1)
  @Max(65535)
  @DisplayMetadata(label = "Port")
  public String port = "443";

  @NotBlank
  @Tag(3)
  @DisplayMetadata(label = "HTTP Path")
  public String httpPath;

  @NotBlank
  @Tag(4)
  @Secret
  @DisplayMetadata(label = "Access Key")
  public String password;

  @Tag(5)
  @DisplayMetadata(label = "Encrypt connection")
  public boolean useSsl = true;

  @Tag(6)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 500;

  public SparkConf() {
  }

  @VisibleForTesting
  private String toJdbcConnectionString() {
    final String password = checkNotNull(this.password, "Missing Access Key.");
    final String portAsString = checkNotNull(this.port, "missing Port");
    final String httpPath = checkNotNull(this.httpPath, "missing HTTP Path");
    final int port = Integer.parseInt(portAsString);

    return String.format("jdbc:spark://%s:%s/default;", hostname, port);
  }

  @Override
  @VisibleForTesting
  protected Config toPluginConfig(CredentialsService credentialsService, OptionManager optionManager) {
    return JdbcStoragePlugin.Config.newBuilder()
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        //.addHiddenSchema("SYSTEM")
        .build();
  }

  private CloseableDataSource newDataSource() {
    final Properties properties = new Properties();

    properties.setProperty("transportMode", "http");
    properties.setProperty("httpPath", httpPath);
    properties.setProperty("AuthMech", "3");

    if (useSsl) {
      properties.setProperty("SSL", "1");
    }

  return DataSources.newGenericConnectionPoolDataSource(
    DRIVER,
    toJdbcConnectionString(),
    "token",
    password,
    properties,
    DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
}

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
