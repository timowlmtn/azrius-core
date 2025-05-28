import subprocess
import re
import psycopg2
import os


def get_lat_lon_from_corelocationcli():
    try:
        result = subprocess.run(
            ["CoreLocationCLI"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
        output = result.stdout

        print(f"[INFO] CoreLocationCLI output: {output.strip()}")

        # Match space-separated floats, e.g., "41.929065 -71.451711"
        match = re.search(r"([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)", output)
        if match:
            latitude = float(match.group(1))
            longitude = float(match.group(2))
            return latitude, longitude
        else:
            raise ValueError("Could not parse lat/lon from output")

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to run CoreLocationCLI: {e.stderr}")
        return None, None
    except Exception as e:
        print(f"[ERROR] Parsing error: {e}")
        return None, None


def ensure_location_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'web' AND table_name = 'location'
            );
        """
        )
        exists = cur.fetchone()[0]

        if not exists:
            print("[INFO] 'web.location' table does not exist. Creating it...")
            cur.execute(
                """
                CREATE TABLE web.location (
                    id SERIAL PRIMARY KEY,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,
                    name TEXT NOT NULL
                );
            """
            )
            conn.commit()
        else:
            print("[INFO] 'web.location' table already exists.")


def insert_location(conn, latitude, longitude, name):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO web.location (latitude, longitude, name)
            VALUES (%s, %s, %s);
        """,
            (latitude, longitude, name),
        )
        conn.commit()
        print(f"[INFO] Location '{name}' saved to database.")


def main():
    latitude, longitude = get_lat_lon_from_corelocationcli()
    if latitude is None or longitude is None:
        print("[ERROR] Failed to resolve location. Exiting.")
        return

    conn = psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
    )

    try:
        ensure_location_table(conn)

        # Check for existing entry
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name FROM web.location
                WHERE ABS(latitude - %s) < 0.0001 AND ABS(longitude - %s) < 0.0001
                LIMIT 1;
            """,
                (latitude, longitude),
            )
            result = cur.fetchone()

        if result:
            print(f"[INFO] Current location already exists: '{result[0]}'")
        else:
            name = input(
                f"Enter a name for this location (lat={latitude}, lon={longitude}): "
            ).strip()
            insert_location(conn, latitude, longitude, name)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
