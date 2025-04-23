import os
import kagglehub

# Đường dẫn thư mục chính
base_path = "/opt/airflow/dataset"

def extract_and_load_to_staging(**kwargs):
    # Đặt biến môi trường để KaggleHub lưu cache vào đúng thư mục
    os.environ["KAGGLEHUB_CACHE"] = base_path

    # Tải dataset về thư mục mặc định
    dataset_path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")
    return dataset_path


