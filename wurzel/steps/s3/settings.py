# SPDX-FileCopyrightText: 2025 Deutsche Telekom AG (opensource@telekom.de)
#
# SPDX-License-Identifier: Apache-2.0

"""Settings for the S3 markdown sink step."""

from pydantic import Field, model_validator

from wurzel.step.settings import Settings


class S3MarkdownStepSettings(Settings):
    """Configuration for ``S3MarkdownStep``.

    Set ``S3MARKDOWNSTEP__SKIP=true`` to make the step a no-op (passthrough only, no S3
    calls, no credentials required) — useful in lower environments that should not write.

    AWS credentials are read from the standard boto3 provider chain (env vars
    ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``, instance/IRSA role, etc.).

    Environment Variables (with S3MARKDOWNSTEP prefix):
        S3MARKDOWNSTEP__SKIP:         When true, skip processing (default: false)
        S3MARKDOWNSTEP__BUCKET:       Target S3 bucket (required when not SKIP)
        S3MARKDOWNSTEP__PREFIX:       Key prefix (default: "dt-cz")
        S3MARKDOWNSTEP__TENANT:       Provenance tenant tag (default: = PREFIX)
        S3MARKDOWNSTEP__REGION:       AWS region (default: "eu-central-1")
        S3MARKDOWNSTEP__ENDPOINT_URL: Override endpoint (MinIO/localstack tests only)
    """

    SKIP: bool = Field(
        default=False,
        description="When true, the step skips the S3 write and passes input through unchanged.",
    )
    BUCKET: str = Field(
        default="",
        description="Target S3 bucket (required when SKIP=false)",
    )
    PREFIX: str = Field(
        default="dt-cz",
        description="Key prefix; objects land at <PREFIX>/<ts>.json and <PREFIX>/latest.json",
    )
    TENANT: str = Field(
        default="",
        description="Provenance tenant tag written as x-amz-meta-tenant (defaults to PREFIX)",
    )
    REGION: str = Field(
        default="eu-central-1",
        description="AWS region for the S3 client",
    )
    ENDPOINT_URL: str = Field(
        default="",
        description="Custom S3 endpoint URL — set only for MinIO / localstack tests",
    )

    @model_validator(mode="after")
    def _require_bucket_unless_skipped(self) -> "S3MarkdownStepSettings":
        if not self.SKIP and not self.BUCKET:
            raise ValueError("S3MarkdownStep is active (SKIP=false) but S3MARKDOWNSTEP__BUCKET is not set")
        return self
