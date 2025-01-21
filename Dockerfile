FROM quay.io/astronomer/astro-runtime:12.5.0

# # Install build dependencies as root
# USER root
# RUN apt-get update && apt-get install -y \
#     python3-dev \
#     build-essential \
#     libatlas-base-dev \
#     gfortran \
#     libopenblas-dev \
#     liblapack-dev \
#     && rm -rf /var/lib/apt/lists/*

# # Switch back to the airflow user
# USER astro

# # Install PySpark globally for the airflow user
# RUN pip install pyspark papermill
# RUN pip install notebook


# # Create and activate a virtual environment, then install dependencies
# RUN python -m venv /home/astro/soda_venv && \
#     /home/astro/soda_venv/bin/pip install --no-cache-dir soda-core-bigquery==3.0.45 && \
#     /home/astro/soda_venv/bin/pip install --no-cache-dir soda-core-scientific==3.0.45

# # Set the virtual environment as the default Python environment
# ENV PATH="/home/astro/soda_venv/bin:$PATH"

# # install soda into a virtual environment
# RUN python -m venv soda_venv && source soda_venv/bin/activate && \
#     pip install --no-cache-dir soda-core-bigquery==3.0.45 &&\
#     pip install --no-cache-dir soda-core-scientific==3.0.45 && deactivate

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt