/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_inhouse;

import io.airbyte.integrations.base.adaptive.AdaptiveDestinationRunner;

public class S3DestinationRunner {

  public static void main(final String[] args) throws Exception {
    AdaptiveDestinationRunner.baseOnEnv()
        .withOssDestination(S3InhouseDestination::new)
        .withCloudDestination(S3DestinationStrictEncrypt::new)
        .run(args);
  }

}
