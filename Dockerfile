# Java 21 (Temurin) is the one heavy host dependency Spark needs; the image
# bakes it in. uv then manages Python 3.12 (from .python-version) and PySpark,
# which bundles its own Spark distribution.
FROM eclipse-temurin:21-jdk

# uv, copied from its official distroless image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Use the locked environment as-is; never re-resolve at runtime.
ENV UV_FROZEN=1

WORKDIR /app
COPY . .
RUN uv sync

# `uv run <script>` is the same command used on the host.
ENTRYPOINT ["uv", "run"]
CMD ["examples/01_spark_session.py"]
