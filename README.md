# Smart Data Warehouse with Automated ETL: E-Commerce Data Analysis Pipeline

This project implements an end-to-end data analysis pipeline for e-commerce data, automating the extraction, transformation, and loading (ETL) processes. The pipeline stages include downloading data, uploading it to Google Cloud Storage (GCS), transforming and loading it into BigQuery, and performing data validation checks for quality assurance. For visualization, Looker Studio is used to create dynamic reports and dashboards based on the BigQuery tables.

## Project Overview

1. **Download Data**: E-commerce data is downloaded from Kaggle to the local machine.
2. **Upload to GCS**: A DAG (Directed Acyclic Graph) in Apache Airflow uploads the data from the local machine to Google Cloud Storage.
3. **Load to BigQuery**: Another DAG in Apache Airflow loads the data from GCS to BigQuery for analysis.
4. **Row Count Checks**: Validates row-level data consistency between the source CSVs in GCS and the BigQuery tables.
5. **Column Check**: Validates column-level data consistency by comparing metrics between the source CSVs in GCS and the BigQuery tables.
6. **Data Visulaization**: Utilize Looker Studio to visualize the BigQuery tables according to specific analytical needs.

## Prerequisites

- Python 3.x
- Apache Airflow
- Google Cloud SDK
- Kaggle API
- Astronomer CLI
- Docker
- Looker Studio

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

3. **Run Row Count Checks**: Trigger the DAG to get the row count and store the results in BQ Table.
The results of the checks will be stored in GCS.

4. **Run Column Checks**: Trigger the DAG to get the Column Quality Check and store the results in BQ Table.
The results of the checks will be stored in GCS.

### 5. DAGs Created
1. **ecommerce_upload_csv_to_gcs**: Uploads data from the local machine to Google Cloud Storage (GCS).
2. **ecommerce_load_gcs_to_bigquery**: Loads the data from GCS to BigQuery for analysis.
3. **ecommerce_row_count_check**: Validates row-level data consistency.
4. **ecommerce_column_quality_check**: Validates column-level data consistency.


### 5. DAG Details
1. **DAG: ecommerce_upload_csv_to_gcs**
- Purpose: Upload CSV files from a local dataset folder to a GCS bucket.
- Tasks:
    - Load environment variables.
    - List CSV files in the local dataset folder.
    - Upload files to GCS using LocalFilesystemToGCSOperator.
- Key Features: Independent processing of each CSV file with logging for success or failure.

2. **DAG: ecommerce_load_gcs_to_bigquery**
- Purpose: Sequentially load CSV files from GCS into BigQuery.
- Tasks:
    - Create a BigQuery dataset.
    - List and filter CSV files in GCS.
    - Upload files to BigQuery using DataFrame.to_gbq().
- Key Features: Sequential execution with logging for monitoring.

3. **DAG: ecommerce_row_count_check**
- Purpose: Validate data quality by comparing row counts.
- Tasks:
    - Create a dataset and table for results.
    - List CSV files in GCS.
    - Compare row counts and log results.
- Key Features: Logs discrepancies for quality validation.

4. **DAG: ecommerce_column_quality_check**
- Purpose: Validate column-level data quality.
- Tasks:
    - Create or update the quality results table.
    - List CSV files and BigQuery tables.
    - Perform column checks and log results.
- Key Features: Detailed insights into column consistency.

### 6. Data Visualization
Utilized Looker Studio to visualize the BigQuery tables according to specific analytical needs. Create dynamic reports and dashboards to gain insights from the e-commerce data.

### License

This project is licensed under the [MIT License](LICENSE). 