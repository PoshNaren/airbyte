#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_google_content_api_inhouse import SourceGoogleContentApiInhouse

if __name__ == "__main__":
    source = SourceGoogleContentApiInhouse()
    launch(source, sys.argv[1:])
