import os
import logging
import sys
from flask_caching.backends.rediscache import RedisCache
from celery.schedules import crontab
from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH

SUPERSET_ENV = os.getenv("SUPERSET_ENV", "") or "development"
SUPERSET_WEBAPP_DOMAIN_NAME = os.getenv("SUPERSET_WEBAPP_DOMAIN_NAME", "") or "localhost"
APP_ROOT = os.getenv("SUPERSET_APP_ROOT", "") or "/"
BASEPATH = os.getenv("BASEPATH", "") or "/"
LOG_LEVEL = getattr(logging, (os.getenv("SUPERSET_LOG_LEVEL", "") or "INFO").upper(), logging.INFO)
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "") or "YOUR_OWN_RANDOM_GENERATED_SECRET_KEY"

# FAB Rate limiting: this is a security feature for preventing DDOS attacks. The
# feature is on by default to make Superset secure by default, but you should
# fine tune the limits to your needs. You can read more about the different
# parameters here: https://flask-limiter.readthedocs.io/en/stable/configuration.html
RATELIMIT_ENABLED = SUPERSET_ENV == "production"
RATELIMIT_APPLICATION = "50 per second"
AUTH_RATE_LIMITED = SUPERSET_ENV == "production"
AUTH_RATE_LIMIT = "5 per second"


WEBDRIVER_BASEURL = f"https://{SUPERSET_WEBAPP_DOMAIN_NAME}{APP_ROOT}/"   # When using docker compose baseurl should be http://superset_nginx{ENV{BASEPATH}}/  # noqa: E501
WEBDRIVER_BASEURL_USER_FRIENDLY = f"https://{SUPERSET_WEBAPP_DOMAIN_NAME}{BASEPATH}/" # The base URL for the email report hyperlinks.

# Superset specific config
ROW_LIMIT = 50000

# Results backend configuration (for caching query results, etc.)
REDIS_HOST = os.getenv("SUPERSET_REDIS_HOST", "") or "redis"
REDIS_PORT = os.getenv("SUPERSET_REDIS_PORT", "") or "6379"
REDIS_CELERY_DB = os.getenv("SUPERSET_REDIS_CELERY_DB", "") or "0"
REDIS_RESULTS_DB = os.getenv("SUPERSET_REDIS_RESULTS_DB", "") or "1"
RESULTS_BACKEND = RedisCache(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_RESULTS_DB, key_prefix='superset_results_')

# Flask-WTF configuration (for CSRF protection)
WTF_CSRF_ENABLED = True # Flask-WTF flag for CSRF
WTF_CSRF_EXEMPT_LIST = [] # Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365 # A CSRF token that expires in 1 year

# Reverse proxy configuration
ENABLE_PROXY_FIX = True

# Medadata database connection string (holds dashboard, queries, etc.)
SUPERSET_POSTGRES_USER = os.getenv("SUPERSET_POSTGRES_USER", "") or "superset"
SUPERSET_POSTGRES_PASSWORD = os.getenv("SUPERSET_POSTGRES_PASSWORD", "") or "superset"
SUPERSET_POSTGRES_HOST = os.getenv("SUPERSET_POSTGRES_HOST", "") or "superset_postgres"
SUPERSET_POSTGRES_PORT = os.getenv("SUPERSET_POSTGRES_PORT", "") or "5432"
SUPERSET_POSTGRES_DB = os.getenv("SUPERSET_POSTGRES_DB", "") or "superset"
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://"
    f"{SUPERSET_POSTGRES_USER}:{SUPERSET_POSTGRES_PASSWORD}@"
    f"{SUPERSET_POSTGRES_HOST}:{SUPERSET_POSTGRES_PORT}/{SUPERSET_POSTGRES_DB}"
)
SQLALCHEMY_EXAMPLES_URI = None

# Redis cache configuration (for caching query results, etc.)
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_RESULTS_DB,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
THUMBNAIL_CACHE_CONFIG = CACHE_CONFIG

# Celery configuration (for background queries execution, scheduled reports, etc.)
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
        "superset.tasks.thumbnails",
        "superset.tasks.cache",
    )
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=10, hour=0),
        },
    }

CELERY_CONFIG = CeleryConfig

# Feature flags (for enabling/disabling features)
FEATURE_FLAGS = {"ALERT_REPORTS": True}
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True
SQLLAB_CTAS_NO_LIMIT = False

# Superset 6.0 moved List Roles to the frontend; required for FAB 5.
FAB_ADD_SECURITY_API = True

# Authentication Configuration
if SUPERSET_ENV == "production":
    # Production: GitHub OAuth authentication
    AUTH_TYPE = AUTH_OAUTH
    
    # Import custom security manager for GitHub OAuth
    from github_security_manager import CustomSecurityManager
    CUSTOM_SECURITY_MANAGER = CustomSecurityManager
    
    # Enable automatic user registration on first OAuth login
    AUTH_USER_REGISTRATION = True
    
    # Default role assigned to newly registered users
    AUTH_USER_REGISTRATION_ROLE = os.getenv("SUPERSET_AUTH_USER_REGISTRATION_ROLE", "") or "Gamma"
    
    # GitHub OAuth Configuration
    GITHUB_CLIENT_ID = os.getenv("SUPERSET_GITHUB_CLIENT_ID", "") or ""
    GITHUB_CLIENT_SECRET = os.getenv("SUPERSET_GITHUB_CLIENT_SECRET", "") or ""
    GITHUB_API_BASE_URL = os.getenv("SUPERSET_GITHUB_API_BASE_URL", "") or "https://api.github.com/"
    GITHUB_ACCESS_TOKEN_URL = os.getenv("SUPERSET_GITHUB_ACCESS_TOKEN_URL", "") or "https://github.com/login/oauth/access_token"
    GITHUB_AUTHORIZE_URL = os.getenv("SUPERSET_GITHUB_AUTHORIZE_URL", "") or "https://github.com/login/oauth/authorize"
    GITHUB_ALLOWED_ORG = os.getenv("SUPERSET_GITHUB_ALLOWED_ORG", "") or ""
    
    # OAuth Providers Configuration
    OAUTH_PROVIDERS = [
        {
            "name": "github",
            "icon": "fa-github",
            "token_key": "access_token",
            "remote_app": {
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "api_base_url": GITHUB_API_BASE_URL,
                "client_kwargs": {
                    "scope": "read:org",
                },
                "authorize_url": GITHUB_AUTHORIZE_URL,
                "access_token_url": GITHUB_ACCESS_TOKEN_URL,
                "request_token_url": None,
            },
        }
    ]
else:
    # Development: Database (password) authentication
    AUTH_TYPE = AUTH_DB
    AUTH_USER_REGISTRATION = False