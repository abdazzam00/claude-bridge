import psycopg2

conn = psycopg2.connect("postgresql://neondb_owner:npg_os5ufYH2maDi@ep-empty-math-aie6cjws.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS bridge_search_requests (
        id SERIAL PRIMARY KEY,
        job_title TEXT,
        companies JSONB,
        location TEXT,
        keywords TEXT,
        max_results INTEGER DEFAULT 10,
        status TEXT DEFAULT 'pending',
        requested_at TIMESTAMPTZ DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        result_count INTEGER DEFAULT 0,
        error TEXT
    )
""")
conn.commit()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
for row in cur.fetchall():
    print(f"Table: {row[0]}")

cur.execute("SELECT COUNT(*) FROM bridge_search_requests WHERE status = 'pending'")
print(f"Pending requests: {cur.fetchone()[0]}")

cur.close()
conn.close()
print("Done")
