# Duolingo-style DAU analytics

Source article: https://blog.duolingo.com/growth-model-duolingo/

## Run yourself

Create a `.env` file:
```
DATABASE_URL="sqlite:///daulingo.db"
USER_STATES_API_BASE="http://localhost:8000/v1"
```

Run the follwing commands:
```bash
pip install -e .

python ./scripts/upload_activity_csv.py
python ./scripts/run_backend.py
python ./scripts/run_frontend.py
```

![image](/resources/screenshot.png)