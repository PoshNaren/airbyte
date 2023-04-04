/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_inhouse;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.airbyte.integrations.destination.s3.credential.S3CredentialConfig;
import io.airbyte.integrations.destination.s3.BaseS3Destination;
import io.airbyte.integrations.destination.s3.S3DestinationConfig;
import io.airbyte.integrations.destination.s3.S3DestinationConfigFactory;
import io.airbyte.integrations.destination.s3.StorageProvider;
import io.airbyte.integrations.destination.s3.S3BaseChecks;
import io.airbyte.integrations.destination.s3.S3FormatConfigs;
import io.airbyte.integrations.destination.s3.S3FormatConfig;
import io.airbyte.integrations.destination.s3.S3StorageOperations;
import io.airbyte.integrations.destination.s3.SerializedBufferFactory;
import io.airbyte.integrations.destination.s3.S3ConsumerFactory;
import io.airbyte.integrations.destination.s3.S3DestinationConstants;
import io.airbyte.integrations.destination.s3.util.S3NameTransformer;
import io.airbyte.integrations.destination.record_buffer.FileBuffer;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.integrations.base.IntegrationRunner;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.airbyte.integrations.destination.s3.constant.S3Constants.ACCESS_KEY_ID;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.ACCOUNT_ID;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.FILE_NAME_PATTERN;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.SECRET_ACCESS_KEY;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.S_3_BUCKET_NAME;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.S_3_BUCKET_PATH;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.S_3_BUCKET_REGION;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.S_3_ENDPOINT;
import static io.airbyte.integrations.destination.s3.constant.S3Constants.S_3_PATH_FORMAT;

public class S3InhouseDestination extends BaseS3Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3InhouseDestination.class);
  public S3InhouseDestination() {}

  @VisibleForTesting
  protected S3InhouseDestination(final S3DestinationConfigFactory s3DestinationConfigFactory) {
    super(s3DestinationConfigFactory);
  }

  public static void main(final String[] args) throws Exception {
    new IntegrationRunner(new S3InhouseDestination()).run(args);
  }

  @Override
  public StorageProvider storageProvider() {
    return StorageProvider.AWS_S3;
  }

  /**
   * Checks if the connection to the S3 bucket based on the user's input (config)
   * is possible.
   *
   * 1. Checks if it has ListObject permission
   * 2. Tests if the given credentials allow for single file uploading
   * 3. Tests if the given credentials allow for multi part uploading
   * @param config
   * @return AirbyteConnectionStatus
   */
  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    try {

      final S3DestinationConfig destinationConfig = configureS3DestinationConfig(config);
      final AmazonS3 s3Client = destinationConfig.getS3Client();

      S3BaseChecks.testIAMUserHasListObjectPermission(s3Client, destinationConfig.getBucketName());
      S3BaseChecks.testSingleUpload(s3Client, destinationConfig.getBucketName(), destinationConfig.getBucketPath());
      S3BaseChecks.testMultipartUpload(s3Client, destinationConfig.getBucketName(), destinationConfig.getBucketPath());

      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (final Exception e) {

      return new AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.FAILED)
              .withMessage("Could not connect to the S3 bucket with the provided configuration. \n" + e
                      .getMessage());
    }
  }

  /**
   * Implementation for the "write" part of the Airbyte destination connector.
   * Takes in the user configuration in the JSON format, the catalog contains
   * a list of all the airbyte streams of data that the source emanates,
   * the outputRecordCollector consumes AirbyteMessages
   *
   * @param config
   * @param catalog
   * @param outputRecordCollector
   * @return
   */
  @Override
  public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                            final ConfiguredAirbyteCatalog catalog,
                                            final Consumer<AirbyteMessage> outputRecordCollector) {

    final S3DestinationConfig s3Config = configureS3DestinationConfig(config);
    final S3NameTransformer s3NameTransformer =  new S3NameTransformer();
    return new S3ConsumerFactory().create(
            outputRecordCollector,
            new S3StorageOperations(s3NameTransformer, s3Config.getS3Client(), s3Config),
            s3NameTransformer,
            SerializedBufferFactory.getCreateFunction(s3Config, FileBuffer::new),
            s3Config,
            catalog);
  }

  /**
   * Constructs a new S3DestinationConfig based on the values provided by the user in the UI
   * or the values in the config.json file
   * @param config
   * @return S3DestinationConfig
   */
  private S3DestinationConfig configureS3DestinationConfig(JsonNode config){

    // Assumes access_key, secret_access_key, and session_token is always given as parameters
    String accessKey = config.get(ACCESS_KEY_ID).asText();
    String secretAccessKey = config.get(SECRET_ACCESS_KEY).asText();
    String sessionToken = config.get("secret_token").asText();

    // Since its assumed that session_token is always given as a parameter
    // Depending on the need, this bit of code might have to be updated based on the needs.
    // For example, an IAM role could be used to gain access to the S3 buckets.
    S3CredentialConfig credentialsProvider = new S3SessionAccessKeyCredentialConfig(accessKey, secretAccessKey, sessionToken);

    String s3Endpoint = "";
    if (config.has(S_3_ENDPOINT)){
      s3Endpoint = config.get(S_3_ENDPOINT).asText();
    }

    // bucketRegion, bucketName, bucketPath, and formatConfig (not pathFormat) are required arguments
    String bucketRegion = config.get(S_3_BUCKET_REGION).asText();
    String bucketName = config.get(S_3_BUCKET_NAME).asText();
    String bucketPath = config.get(S_3_BUCKET_PATH).asText();

    String pathFormat = S3DestinationConstants.DEFAULT_PATH_FORMAT;
    if (config.has(S_3_PATH_FORMAT)){
      pathFormat = config.get(S_3_PATH_FORMAT).asText();
    }

    // throws an error if the config file doesn't have a "format" key
    S3FormatConfig formatConfig = S3FormatConfigs.getS3FormatConfig(config);
    if(config.has("format")){
      formatConfig = S3FormatConfigs.getS3FormatConfig(config);
    }

    String fileNamePattern = S3DestinationConstants.DEFAULT_PATH_FORMAT;
    if(config.has(FILE_NAME_PATTERN)){
      fileNamePattern = config.get(FILE_NAME_PATTERN).asText();
    }

    boolean checkIntegrity = true;
    int uploadThreadsCount = S3StorageOperations.DEFAULT_UPLOAD_THREADS;

    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion(bucketRegion)
            .withCredentials(credentialsProvider.getS3CredentialsProvider())
            .build();

    return new S3DestinationConfig(s3Endpoint,
            bucketName,
            bucketPath,
            bucketRegion,
            pathFormat,
            credentialsProvider,
            formatConfig,
            s3Client,
            fileNamePattern,
            checkIntegrity,
            uploadThreadsCount);
  }

}
