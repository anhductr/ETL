import os
import kagglehub
import shutil

# Đường dẫn thư mục chính
base_path = "/opt/airflow/dataset"

def extract_and_load_to_staging():
    # Đặt biến môi trường để KaggleHub lưu cache vào đúng thư mục
    os.environ["KAGGLEHUB_CACHE"] = base_path

    # Tải dataset về thư mục mặc định
    dataset_path = kagglehub.dataset_download("rohanrao/formula-1-world-championship-1950-2020")

    # Duyệt qua tất cả thư mục và tệp trong dataset_path
    for root, dirs, files in os.walk(dataset_path):
        # Kiểm tra và di chuyển các tệp CSV
        for file in files:
            print(f"Đang xử lý tệp: {file}")
            if file.endswith(".csv"):  # Chỉ di chuyển file CSV
                old_file_path = os.path.join(root, file)
                new_file_path = os.path.join(base_path, file)

                # Kiểm tra nếu tệp đã tồn tại trong thư mục đích
                if not os.path.exists(new_file_path):
                    # Di chuyển file CSV về thư mục base_path
                    shutil.move(old_file_path, new_file_path)
                    print(f"📂 Đã di chuyển {file} về {base_path}")
                else:
                    print(f"📂 Tệp {file} đã tồn tại tại {base_path}, không di chuyển.")
