# E-Commerce Data Analysis Pipeline

This project implements an end-to-end data analysis pipeline for e-commerce data. The pipeline involves downloading data from Kaggle, uploading it to Google Cloud Storage (GCS), and loading it into BigQuery for further analysis. Apache Airflow is used for orchestration, and the project is initialized using Astronomer.

Additionally, the project includes data quality checks implemented using SQL, which are orchestrated by an Airflow DAG. The results of these checks are stored in GCS for further review.

## Project Overview

1. **Download Data**: E-commerce data is downloaded from Kaggle to the local machine.
2. **Upload to GCS**: A DAG (Directed Acyclic Graph) in Apache Airflow uploads the data from the local machine to Google Cloud Storage.
3. **Load to BigQuery**: Another DAG in Apache Airflow loads the data from GCS to BigQuery for analysis.
4. **Data Quality Checks**: SQL-based data quality checks are implemented and orchestrated by an Airflow DAG. The results of these checks are stored in GCS.

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
1. Create a Service Account:
    - Go to the Google Cloud Console.
    - Navigate to IAM & Admin > Service Accounts.
    - Click Create Service Account.
    - Provide a name and description for the service account.
    - Click Create.

2. Set Up Permissions:

    - Assign the following roles to the service account:
        - BigQuery Admin
        - Storage Admin
    - Click Continue and then Done.

3. Download the Credentials File:

    - Click on the created service account.
    - Navigate to the Keys tab.
    - Click Add Key > Create New Key.
    - Select JSON and click Create.
    - Download the credentials.json file.
    - Place the Credentials File:

### 4. Set Up Google Cloud Credentials
Place your Google Cloud credentials.json file in the include/gcp folder.

```sh
mkdir -p include/gcp
mv /path/to/your/credentials.json include/gcp/service_account.json
```

### 5. Download Data from Kaggle
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

### 4. Trigger the DAGs
1. **Upload Data to GCS**: Trigger the DAG to upload data from your local machine to GCS.

2. **Load Data to BigQuery**: Trigger the DAG to load data from GCS to BigQuery.

3. **Run Data Quality Checks**: Trigger the DAG to execute all SQL-based data quality checks.
The results of the checks will be stored in GCS.


## Data Quality Checks (SQL-Based Checks)
The following SQL-based data quality checks are implemented:

**Schema Validation**: Ensures the table structure matches the expected schema.

**Completeness Check**: Ensures no mandatory fields are missing.

**Uniqueness Check**: Detects duplicate records.

**Null Value Check**: Identifies and handles null values in critical fields.

**Consistency Check**: Validates data against business rules.

**Timeliness Check**: Ensures data is ingested on time.

**Airflow DAG for Data Quality Checks**: A dedicated Airflow DAG orchestrates the execution of all SQL checks.
The results of the checks are written to a CSV file and stored in GCS for further review.

### License

This project is licensed under the [MIT License](LICENSE). 