FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# make new directory for code and set it as working directory
WORKDIR /code 
# Add virtual environment to PATH so we can use installed packages
ENV PATH="/code/.venv/bin:$PATH"
# copy the script and call it using entrypoint

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

COPY data_download.py data_download.py

ENTRYPOINT ["uv","run","python","data_download.py","--output-dir","./data"]
