from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id) #Khởi tạo PostgresHook với conn_id.

    def create_table(self, query):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(query)
            conn.commit()
            print(f"Đã tạo bảng thành công.")
        except Exception as e:
            conn.rollback()
            print("Lỗi khi tạo bảng:", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def insert_table(self, query, values):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.executemany(query, values)
            conn.commit()
            print("Đã transform và lưu dữ liệu vào bảng.")
        except Exception as e:
            conn.rollback()
            print("Lỗi khi insert dữ liệu:", e)
            raise
        finally:
            cursor.close()
            conn.close()
        