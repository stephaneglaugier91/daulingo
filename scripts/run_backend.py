from dotenv import load_dotenv

from backend.config import get_settings
from backend.main import create_app

load_dotenv()
settings = get_settings()
app = create_app(settings)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("run_backend:app", host="127.0.0.1", port=8000, reload=True)
