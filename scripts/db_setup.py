"""Set up the Neon DB schema."""
import os
import psycopg2

NEON_CONN = os.environ.get("NEON_DATABASE_URL", "")

def setup():
    if not NEON_CONN:
        print("[!] Set NEON_DATABASE_URL environment variable first")
        return

    conn = psycopg2.connect(NEON_CONN)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE,
            name TEXT,
            headline TEXT,
            location TEXT,
            about TEXT,
            current_company TEXT,
            experience JSONB,
            education JSONB,
            skills JSONB,
            raw_text TEXT,
            scraped_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_profiles_name ON profiles(name);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("[+] Neon DB schema ready")

if __name__ == "__main__":
    setup()
