FROM mcr.microsoft.com/devcontainers/python:3.14-bookworm

# Install uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install system dependencies needed for FIS and Makefile workflows
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
        make \
        sqlite3 \
        unzip \
        gdal-bin \
        libgdal-dev \
        gh \
        pandoc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set uv environment defaults for containers
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1

# Pre-install Python 3.14 via uv as vscode user
USER vscode
RUN uv python install 3.14

# Set default working directory
WORKDIR /workspaces/fis
