import subprocess
import re


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
        lat_match = re.search(r"Latitude:\s*([-+]?[0-9]*\.?[0-9]+)", output)
        lon_match = re.search(r"Longitude:\s*([-+]?[0-9]*\.?[0-9]+)", output)

        if lat_match and lon_match:
            latitude = lat_match.group(1)
            longitude = lon_match.group(1)
            return latitude, longitude
        else:
            raise ValueError("Could not parse latitude/longitude")

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to run CoreLocationCLI: {e.stderr}")
        return None, None
    except Exception as e:
        print(f"[ERROR] Parsing error: {e}")
        return None, None


def resolve_geo_schema():
    latitude, longitude = get_lat_lon_from_corelocationcli()
    if not latitude or not longitude:
        return "default_schema"

    # Build a schema-safe name
    schema = f"geo_{latitude}_{longitude}".replace(".", "_").replace("-", "neg")
    return schema.lower()


def main():
    schema = resolve_geo_schema()
    print(f"[INFO] Resolved PostgreSQL schema: {schema}")


if __name__ == "__main__":
    main()
