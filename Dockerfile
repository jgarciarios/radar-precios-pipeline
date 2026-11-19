# Imagen propia sobre la oficial de Airflow.
# No usamos _PIP_ADDITIONAL_REQUIREMENTS (variable de entorno de Airflow para
# instalar paquetes al vuelo): la docs la marca explícitamente como "solo para
# probar cosas rápido, no usar en producción" porque reinstala en cada arranque
# de contenedor. Con una imagen propia, la dependencia queda fijada en una capa
# y el build es reproducible.
FROM apache/airflow:2.10.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
