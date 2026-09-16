
FROM apache/superset:6.1.0

USER root

COPY requirements.txt /app/requirements.txt
COPY github_security_manager.py /app/github_security_manager.py

WORKDIR /app

# Install packages using uv into the virtual environment
RUN . /app/.venv/bin/activate && uv pip install -r /app/requirements.txt

WORKDIR /app

USER superset

CMD ["/app/docker/entrypoints/run-server.sh"]