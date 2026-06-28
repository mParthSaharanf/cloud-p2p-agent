FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tracker/ tracker/
COPY agent/ agent/
COPY config/ config/

# Default: run the agent daemon
# Override CMD in docker-compose for the tracker
CMD ["python", "-m", "agent.main"]