FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

COPY services/mcp/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services /app/services
COPY elastic /app/elastic

EXPOSE 7010
CMD ["python", "-m", "services.mcp.server"]
