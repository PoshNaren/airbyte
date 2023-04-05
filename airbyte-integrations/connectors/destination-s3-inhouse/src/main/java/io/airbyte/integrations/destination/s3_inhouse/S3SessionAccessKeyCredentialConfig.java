
package io.airbyte.integrations.destination.s3_inhouse;

import io.airbyte.integrations.destination.s3.credential.S3CredentialConfig;
import io.airbyte.integrations.destination.s3.credential.S3CredentialType;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.AWSCredentials;
public class S3SessionAccessKeyCredentialConfig implements S3CredentialConfig {

    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    public S3SessionAccessKeyCredentialConfig(final String accessKeyId, final String secretAccessKey, final String sessionToken) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
    }

    @Override
    public S3CredentialType getCredentialType() {
        return S3CredentialType.ACCESS_KEY;
    }

    @Override
    public AWSCredentialsProvider getS3CredentialsProvider() {
        final AWSCredentials awsCreds = new BasicSessionCredentials(accessKeyId, secretAccessKey, sessionToken);
        return new AWSStaticCredentialsProvider(awsCreds);
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

}