FROM apache/airflow:2.9.2

# Chạy với quyền root
USER root

# Cài đặt thư viện hệ thống để hỗ trợ biên dịch pandas
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Sao chép file requirements.txt vào container
COPY requirements.txt /requirements.txt

# Chuyển lại sang user airflow sau khi cài xong
USER airflow

# Cài đặt các package từ requirements.txt
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /requirements.txt
