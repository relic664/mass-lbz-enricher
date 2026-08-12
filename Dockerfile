FROM python:3.12-slim
WORKDIR /app
COPY enricher /app/enricher
EXPOSE 8080
CMD ["python", "-m", "enricher.main"]
