import os
import shutil
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from typing import List

app = FastAPI()

# Initialize the GCS client
client = storage.Client()

@app.post("/upload-folder/")
async def upload_folder(files: List[UploadFile] = File(...), gcs_bucket: str = Form(...)):
    # Create a temporary directory to store uploaded files
    temp_dir = "temp_upload"
    os.makedirs(temp_dir, exist_ok=True)

    try:
        # Save uploaded files to the temporary directory
        for file in files:
            file_path = os.path.join(temp_dir, file.filename)
            with open(file_path, "wb") as f:
                shutil.copyfileobj(file.file, f)

        # List all CSV files in the temporary directory
        csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]

        if not csv_files:
            raise HTTPException(status_code=400, detail="No CSV files found in the uploaded files")

        # Create the raw_data folder in the GCS bucket
        bucket = client.bucket(gcs_bucket)
        raw_data_folder_blob = bucket.blob('raw_data/')
        raw_data_folder_blob.upload_from_string('')  # Create an empty object to represent the folder

        # Upload each CSV file to the raw_data folder in the GCS bucket
        for file_name in csv_files:
            local_file_path = os.path.join(temp_dir, file_name)
            gcs_file_path = f'raw_data/{file_name}'
            blob = bucket.blob(gcs_file_path)

            # Check if the file already exists in the bucket
            if blob.exists():
                raise HTTPException(status_code=409, detail=f"File {file_name} already exists in the bucket")

            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {local_file_path} to {gcs_file_path}")

    except GoogleAPIError as e:
        raise HTTPException(status_code=500, detail=f"Google API error: {e.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")
    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)

    return {"message": "Files uploaded successfully", "files": csv_files}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
