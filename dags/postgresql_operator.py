from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id) #Khởi tạo PostgresHook với conn_id.

    def create_table(self, query):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

    