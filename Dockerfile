# ---------- Stage 1: builder ----------
FROM python:3.11-slim AS builder

WORKDIR /app

# Copiamos primero requirements para aprovechar cache
COPY requirements.txt .

# (Opcional) herramientas de build si algún wheel lo requiere
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
 && pip wheel --wheel-dir=/wheels -r requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim

# Usuario no root
RUN useradd -m appuser
WORKDIR /app

# Certificados para TLS
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Instalamos wheels precompilados
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*

# Copiamos el código del proyecto
# (este Dockerfile vive dentro de create_segmeted_supporters_v5)
COPY . .

# Archivo de logs (tu RotatingFileHandler escribe css_logs.log en el cwd)
RUN touch /app/css_logs.log && chown -R appuser:appuser /app

USER appuser

# Evita buffering de stdout (útil para logs en ECS/CloudWatch)
ENV PYTHONUNBUFFERED=1

# Si tu import es services.snowflake_manager_service..., esto te vale:
# asegúrate de que existan __init__.py en cada carpeta de paquete.
ENTRYPOINT ["python", "-u", "main.py"]