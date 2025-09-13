from pathlib import Path

from streamlit.web.bootstrap import run


def create_app():
    app_path = Path(__file__).with_name("ui.py")

    run(str(app_path), False, [], {})
