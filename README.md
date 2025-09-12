# Duolingo-style DAU analytics

Source article: https://blog.duolingo.com/growth-model-duolingo/

Current implementation is a set of scripts which can be run with mocked data. Plan is to flesh out further into some kind of analytics API.

See [/resources/outputs](/resources/outputs).

## Run yourself

```bash
uv pip install -e .

./scripts/run.sh --fake
```

Delete the `daulingo.db` file to reset to an empty database.