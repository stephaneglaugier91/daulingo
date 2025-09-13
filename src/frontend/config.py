import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    backend_url: str


def get_settings() -> Settings:
    url = os.getenv("USER_STATES_API_BASE")
    if not url:
        raise RuntimeError("USER_STATES_API_BASE not set")
    return Settings(backend_url=url)
