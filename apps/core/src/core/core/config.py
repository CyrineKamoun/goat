from pydantic import PostgresDsn, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ------------------------------------------------------------------
    # Application
    # ------------------------------------------------------------------
    PROJECT_NAME: str = "GOAT Core API"
    ENVIRONMENT: str = "dev"
    AUTH: bool = True
    TEST_MODE: bool = False
    API_V2_STR: str = "/api/v2"
    API_URL: str = "http://localhost:8000/api/v2"
    CLIENT_URL: str = "http://localhost:3000"
    MAX_FOLDER_COUNT: int = 100

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_PORT: int = 5432
    ASYNC_SQLALCHEMY_DATABASE_URI: str | None = None

    @field_validator("ASYNC_SQLALCHEMY_DATABASE_URI", mode="after")
    @classmethod
    def assemble_async_db_connection(
        cls: type["Settings"], value: str | None, info: ValidationInfo
    ) -> str:
        if value:
            return value
        return str(
            PostgresDsn.build(
                scheme="postgresql+psycopg",
                username=info.data.get("POSTGRES_USER"),
                password=info.data.get("POSTGRES_PASSWORD"),
                host=info.data.get("POSTGRES_SERVER"),
                port=info.data.get("POSTGRES_PORT"),
                path=f"{info.data.get('POSTGRES_DB') or ''}",
            )
        )

    # ------------------------------------------------------------------
    # Schemas
    # ------------------------------------------------------------------
    SCHEMA: str = "customer"

    # ------------------------------------------------------------------
    # Auth / Keycloak
    # ------------------------------------------------------------------
    KEYCLOAK_SERVER_URL: str | None = "http://auth-keycloak:8080"
    REALM_NAME: str | None = "p4b"
    KEYCLOAK_CLIENT_ID: str | None = None
    KEYCLOAK_CLIENT_SECRET: str | None = None
    # Default identity used when AUTH=False (local dev / self-hosted without
    # Keycloak). Requests without a bearer token act as this user; the user and
    # its organization are seeded by initial_data.
    DEFAULT_USER_ID: str = "744e4fd1-685c-495c-8b02-efebce875359"
    DEFAULT_USER_EMAIL: str = "admin@goat.local"
    DEFAULT_USER_FIRSTNAME: str = "GOAT"
    DEFAULT_USER_LASTNAME: str = "Admin"
    DEFAULT_ORGANIZATION_NAME: str = "GOAT"
    # Plan and quotas applied to organizations when no billing system is
    # configured (self-hosted deployments). With billing enabled these come
    # from the billing provider instead.
    DEFAULT_PLAN_NAME: str = "goat_enterprise"
    DEFAULT_QUOTA_STORAGE_MB: int = 1048576
    DEFAULT_QUOTA_PROJECTS: int = 10000
    DEFAULT_QUOTA_EDITORS: int = 1000
    DEFAULT_QUOTA_VIEWERS: int = 1000

    # ------------------------------------------------------------------
    # Object storage — data bucket (S3-compatible: AWS / Hetzner / MinIO)
    # ------------------------------------------------------------------
    S3_ACCESS_KEY_ID: str | None = None
    S3_SECRET_ACCESS_KEY: str | None = None
    S3_REGION: str | None = "eu-central-1"  # or "fsn1" for Hetzner
    S3_ENDPOINT_URL: str | None = None  # e.g. "https://s3.fsn1.de"; None for AWS
    S3_PUBLIC_ENDPOINT_URL: str | None = None  # e.g. "https://s3.plan4better.de"
    S3_PROVIDER: str | None = "aws"  # "aws" | "hetzner" | "minio"
    S3_FORCE_PATH_STYLE: bool = False  # needed for MinIO
    S3_BUCKET_PATH: str | None = ""  # set depending on ENVIRONMENT
    S3_BUCKET_NAME: str | None = "goat"
    MAX_UPLOAD_DATASET_FILE_SIZE: int = 5 * 1024 * 1024 * 1024  # 5 GB

    # ------------------------------------------------------------------
    # Object storage — assets bucket (AWS; avatars, thumbnails)
    # ------------------------------------------------------------------
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_REGION: str | None = "eu-central-1"
    AWS_S3_ASSETS_BUCKET: str | None = "plan4better-assets"

    # Catalog storage config (local database schema)
    CATALOG_SCHEMA: str = "datacatalog"

    # ------------------------------------------------------------------
    # Assets / thumbnails
    # ------------------------------------------------------------------
    ASSETS_URL: str | None = None
    ASSETS_MAX_FILE_SIZE: int | None = 4194304
    DOCUMENTS_MAX_FILE_SIZE: int = 52428800  # 50 MiB
    DEFAULT_PROJECT_THUMBNAIL: str | None = (
        "https://assets.plan4better.de/img/goat_new_project_artwork.png"
    )
    DEFAULT_LAYER_THUMBNAIL: str | None = (
        "https://assets.plan4better.de/img/goat_new_dataset_thumbnail.png"
    )

    # ------------------------------------------------------------------
    # Default avatars
    # ------------------------------------------------------------------
    USER_DEFAULT_AVATAR: str | None = (
        "https://assets.plan4better.de/img/no-user-thumb.jpg"
    )
    ORGANIZATION_DEFAULT_AVATAR: str | None = (
        "https://assets.plan4better.de/img/no-org-thumb.jpg"
    )

    # ------------------------------------------------------------------
    # Email / SMTP
    # ------------------------------------------------------------------
    SMTP_TLS: bool = True
    SMTP_PORT: int = 587
    SMTP_HOST: str | None = "smtp.office365.com"
    SMTP_USER: str | None = None
    SMTP_PASSWORD: str | None = None
    EMAILS_FROM_NAME: str | None = "Plan4Better - Account"

    # ------------------------------------------------------------------
    # Billing / Stripe
    # ------------------------------------------------------------------
    STRIPE_SECRET_KEY: str | None = None
    STRIPE_WEBHOOK_SECRET: str | None = None

    # ------------------------------------------------------------------
    # GeoAPI
    # ------------------------------------------------------------------
    GOAT_GEOAPI_HOST: str | None = None

    # ------------------------------------------------------------------
    # Custom domains (white label)
    # ------------------------------------------------------------------
    # Customers CNAME their domains at this hostname. We maintain it as a CNAME
    # in the plan4better.de zone pointing at the actual Caddy LoadBalancer's
    # hostname, so the underlying LB can be migrated without breaking customer
    # DNS records.
    CUSTOM_DOMAIN_CNAME_TARGET: str = "cname.goat.plan4better.de"
    # Public resolvers used by white-label DNS reconciliation. We query these
    # directly instead of the pod's resolver so we see DNS the way customers do
    # — bypassing split-DNS setups. Comma-separated list of IPs.
    CUSTOM_DOMAIN_DNS_RESOLVERS: str = "1.1.1.1,8.8.8.8"

    model_config = SettingsConfigDict(case_sensitive=True)


settings = Settings()
