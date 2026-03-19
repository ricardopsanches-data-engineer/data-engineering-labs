import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="testdb",
    user="admin",
    password="admin",
    port=5432
)

print("Conectado ao PostgreSQL!")

cur = conn.cursor()

cur.execute("SELECT version();")
version = cur.fetchone()

print("Versão do banco:", version)

cur.close()
conn.close()