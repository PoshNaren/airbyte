#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_sendgrid_inhouse import SourceSendgridInhouse

if __name__ == "__main__":
    source = SourceSendgridInhouse()
    launch(source, sys.argv[1:])
