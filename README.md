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

### 4. Download Data from Kaggle
Download the e-commerce data from Kaggle to your local machine. You can find the dataset from https://www.kaggle.com/datasets/mmohaiminulislam/ecommerce-data-analysis/data.

### 6. Load Data to BigQuery
Create another DAG in Apache Airflow to load the data from GCS to BigQuery.


## Running the Pipeline
### 1. Start the Airflow web server and scheduler using Docker:

```sh
astro dev start
```

### 2. Access the Airflow UI:

Open your web browser and go to http://localhost:8080.


### 3. Configure the key file path in the Airflow UI:

- Note: In the Airflow UI, inside configs, put the key file path of credentials.json from your Docker container's storage, not your local machine.
- To find the local path of your file in the Docker container, use the following commands:
    ```sh
    astro dev bash
    ls /usr/local/airflow/include/gcp/service-account.json
    ```

- Use the displayed path (e.g., /usr/local/airflow/include/gcp/service_account.json) in your Airflow configuration.

### 4. Trigger the DAGs from the Airflow UI to upload data to GCS and load it into BigQuery.

### License

This project is licensed under the [MIT License](LICENSE). 