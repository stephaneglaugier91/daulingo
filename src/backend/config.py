from functools import lru_cache

from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    """
    Application settings.
    Don't initialise this class directly. Use the get_settings() function instead.
    """

    DATABASE_URL: str

    model_config = SettingsConfigDict(extra="ignore")


@lru_cache
def get_settings() -> Settings:
    """
    Returns the application settings.
    """
    return Settings()
