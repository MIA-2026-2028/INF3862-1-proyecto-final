# Python 3.12 con uv: el entorno se instala desde uv.lock, igual que con uv sync
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# el entorno vive fuera de /proyecto para que el montaje del repositorio no lo oculte
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy

WORKDIR /proyecto
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

CMD ["uv", "run", "--no-sync", "jupyter", "lab", "--ip=0.0.0.0", "--port=8888", \
     "--no-browser", "--allow-root", "--ServerApp.root_dir=/proyecto"]
