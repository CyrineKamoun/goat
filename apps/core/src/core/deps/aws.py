from core.core.config import settings

# Reuse the S3 client built in settings (same AWS credentials/region) instead of
# constructing a second one.
s3_client = settings.S3_CLIENT
