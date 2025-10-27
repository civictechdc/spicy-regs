FROM python:3.12
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY . /app
RUN uv sync --locked
CMD ["uv", "run", "fastapi", "dev", "src/main.py"]
