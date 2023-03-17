/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.redshift_inhouse;

import static io.airbyte.integrations.base.errors.messages.ErrorMessage.getErrorMessage;

import com.fasterxml.jackson.databind.JsonNode;

import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.AirbyteTraceMessageUtility;
import io.airbyte.integrations.base.IntegrationRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.exceptions.ConnectionErrorException;

import io.airbyte.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.integrations.destination.jdbc.JdbcBufferedConsumerFactory;
import io.airbyte.integrations.destination.jdbc.SqlOperations;
import io.airbyte.integrations.destination.redshift.operations.RedshiftSqlOperations;
import io.airbyte.integrations.destination.NamingConventionTransformer;

import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.factory.DatabaseDriver;

import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;

import java.util.UUID;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.function.Consumer;
import java.util.Optional;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftInhouseDestination extends AbstractJdbcDestination implements Destination {

  public static final String DRIVER_CLASS = DatabaseDriver.REDSHIFT.getDriverClassName();
  public static final Map<String, String> SSL_JDBC_PARAMETERS = ImmutableMap.of(
      JdbcUtils.SSL_KEY, "true",
      "sslfactory", "com.amazon.redshift.ssl.NonValidatingFactory");
  
  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftInhouseDestination.class);

  public RedshiftInhouseDestination() {
    super(DRIVER_CLASS, new RedshiftSQLNameTransformer(), new RedshiftSqlOperations());
  }

  @Override
  public JsonNode toJdbcConfig(final JsonNode redshiftConfig) {
    return getJdbcConfig(redshiftConfig);
  }

  @Override
  public DataSource getDataSource(final JsonNode config) {
    final var jdbcConfig = getJdbcConfig(config);
    return DataSourceFactory.create(
        jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText(),
        jdbcConfig.has(JdbcUtils.PASSWORD_KEY) ? jdbcConfig.get(JdbcUtils.PASSWORD_KEY).asText() : null,
        RedshiftInhouseDestination.DRIVER_CLASS,
        jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText(),
        SSL_JDBC_PARAMETERS);
  }

  @Override
  public JdbcDatabase getDatabase(final DataSource dataSource) {
    return new DefaultJdbcDatabase(dataSource);
  }

  @Override
  protected Map<String, String> getDefaultConnectionProperties(final JsonNode config) {
    return SSL_JDBC_PARAMETERS;
  }

  public static JsonNode getJdbcConfig(final JsonNode redshiftConfig) {
    final String schema = Optional.ofNullable(redshiftConfig.get(JdbcUtils.SCHEMA_KEY)).map(JsonNode::asText).orElse("public");
    Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put(JdbcUtils.USERNAME_KEY, redshiftConfig.get(JdbcUtils.USERNAME_KEY).asText())
        .put(JdbcUtils.PASSWORD_KEY, redshiftConfig.get(JdbcUtils.PASSWORD_KEY).asText())
        .put(JdbcUtils.JDBC_URL_KEY, String.format("jdbc:redshift://%s:%s/%s",
            redshiftConfig.get(JdbcUtils.HOST_KEY).asText(),
            redshiftConfig.get(JdbcUtils.PORT_KEY).asText(),
            redshiftConfig.get(JdbcUtils.DATABASE_KEY).asText()))
        .put(JdbcUtils.SCHEMA_KEY, schema);

    if (redshiftConfig.has(JdbcUtils.JDBC_URL_PARAMS_KEY)) {
      configBuilder.put(JdbcUtils.JDBC_URL_PARAMS_KEY, redshiftConfig.get(JdbcUtils.JDBC_URL_PARAMS_KEY));
    }

    return Jsons.jsonNode(configBuilder.build());
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {

    final DataSource dataSource = getDataSource(config);

    try {
      final JdbcDatabase database = getDatabase(dataSource);
      NamingConventionTransformer namingResolver = getNamingResolver();
      final String outputSchema = namingResolver.getIdentifier(config.get(JdbcUtils.SCHEMA_KEY).asText());
      SqlOperations sqlOperations = getSqlOperations();
      attemptSQLCreateAndDropTableOperations(outputSchema, database, namingResolver, sqlOperations);
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (final ConnectionErrorException ex) {
      final String message = getErrorMessage(ex.getStateCode(), ex.getErrorCode(), ex.getExceptionMessage(), ex);
      AirbyteTraceMessageUtility.emitConfigErrorTrace(ex, message);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage(message);
    } catch (final Exception e) {
      LOGGER.error("Exception while checking connection: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect with provided configuration. \n" + e.getMessage());
    } finally {
      try {
        DataSourceFactory.close(dataSource);
      } catch (final Exception e) {
        LOGGER.warn("Unable to close data source.", e);
      }
    }

  }

  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog catalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {
    // TODO
    NamingConventionTransformer namingResolver = getNamingResolver();
    SqlOperations sqlOperations = getSqlOperations();
    return InhouseJdbcBufferedConsumerFactory.create(outputRecordCollector, getDatabase(getDataSource(config)), sqlOperations, namingResolver, config,
        catalog);
  }

   /**
   * This method is deprecated. It verifies table creation, but not insert right to a newly created
   * table. Use attemptTableOperations with the attemptInsert argument instead.
   */
   // TODO: Have to remove the create and drop table operations
  @Deprecated
  public static void attemptSQLCreateAndDropTableOperations(final String outputSchema,
                                                            final JdbcDatabase database,
                                                            final NamingConventionTransformer namingResolver,
                                                            final SqlOperations sqlOps)
      throws Exception {
    attemptTableOperations(outputSchema, database, namingResolver, sqlOps, false);
  }

  /**
   * Verifies if provided creds has enough permissions. Steps are: 1. Create schema if not exists. 2.
   * Create test table. 3. Insert dummy record to newly created table if "attemptInsert" set to true.
   * 4. Delete table created on step 2.
   *
   * @param outputSchema - schema to tests against.
   * @param database - database to tests against.
   * @param namingResolver - naming resolver.
   * @param sqlOps - SqlOperations object
   * @param attemptInsert - set true if need to make attempt to insert dummy records to newly created
   *        table. Set false to skip insert step.
   * @throws Exception
   */
  public static void attemptTableOperations(final String outputSchema,
                                            final JdbcDatabase database,
                                            final NamingConventionTransformer namingResolver,
                                            final SqlOperations sqlOps,
                                            final boolean attemptInsert)
      throws Exception {
    // verify we have write permissions on the target schema by creating a table with a random name,
    // then dropping that table
    try {
      // Get metadata from the database to see whether connection is possible
      database.bufferedResultSetQuery(conn -> conn.getMetaData().getCatalogs(), JdbcUtils.getDefaultSourceOperations()::rowToJson);

      // verify we have write permissions on the target schema by creating a table with a random name,
      // then dropping that table
      final String outputTableName = namingResolver.getIdentifier("_airbyte_connection_test_" + UUID.randomUUID().toString().replaceAll("-", ""));
      // LOGGER.info("Attempting to create a schema", outputSchema);
      // sqlOps.createSchemaIfNotExists(database, outputSchema);
      LOGGER.info("Attempting to create a table", outputTableName);
      sqlOps.createTableIfNotExists(database, outputSchema, outputTableName);
      // verify if user has permission to make SQL INSERT queries
      try {
        if (attemptInsert) {
          sqlOps.insertRecords(database, List.of(getDummyRecord()), outputSchema, outputTableName);
        }
      } finally {
        sqlOps.dropTableIfExists(database, outputSchema, outputTableName);
      }
    } catch (final SQLException e) {
      if (Objects.isNull(e.getCause()) || !(e.getCause() instanceof SQLException)) {
        throw new ConnectionErrorException(e.getSQLState(), e.getErrorCode(), e.getMessage(), e);
      } else {
        final SQLException cause = (SQLException) e.getCause();
        throw new ConnectionErrorException(e.getSQLState(), cause.getErrorCode(), cause.getMessage(), e);
      }
    } catch (final Exception e) {
      throw new Exception(e);
    }
  }

  private static AirbyteRecordMessage getDummyRecord() {
    final JsonNode dummyDataToInsert = Jsons.deserialize("{ \"field1\": true }");
    return new AirbyteRecordMessage()
        .withStream("stream1")
        .withData(dummyDataToInsert)
        .withEmittedAt(1602637589000L);
  }

  public static void main(String[] args) throws Exception {
    final Destination destination = new RedshiftInhouseDestination();
    LOGGER.info("starting destination: {}", RedshiftInhouseDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("completed destination: {}", RedshiftInhouseDestination.class);
  }

}
