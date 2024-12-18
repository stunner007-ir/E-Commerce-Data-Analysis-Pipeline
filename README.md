# E-Commerce Data Analysis Pipeline

This project implements an end-to-end data analysis pipeline for e-commerce data. The pipeline involves downloading data from Kaggle, uploading it to Google Cloud Storage (GCS), and loading it into BigQuery for further analysis. Apache Airflow is used for orchestration, and the project is initialized using Astronomer.

## Project Overview

1. **Download Data**: E-commerce data is downloaded from Kaggle to the local machine.
2. **Upload to GCS**: A DAG (Directed Acyclic Graph) in Apache Airflow uploads the data from the local machine to Google Cloud Storage.
3. **Load to BigQuery**: Another DAG in Apache Airflow loads the data from GCS to BigQuery for analysis.

## Prerequisites

- Python 3.x
- Apache Airflow
- Google Cloud SDK
- Kaggle API
- Astronomer CLI
- Docker

## Setup

### 1. Clone the Repository

Clone the repository to your local machine.

```sh
git clone https://github.com/stunner007-ir/E-Commerce-Data-Analysis-Pipeline.git
cd E-Commerce-Data-Analysis-Pipeline
```

### 2. Initialize the Project with Astronomer
Ensure you have the Astronomer CLI installed. Initialize the project using Astronomer.

```sh
astro dev init
```

### 3. Set Up Google Cloud Credentials
Place your Google Cloud credentials.json file in the include/gcp folder.

```sh
mkdir -p include/gcp
mv /path/to/your/credentials.json include/gcp/service_account.json
```

