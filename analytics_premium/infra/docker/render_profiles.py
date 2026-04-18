import json
import os
import sys


def build_output() -> dict:
    output_json = os.getenv("DBT_TARGET_OUTPUT_JSON")
    if output_json:
        output = json.loads(output_json)
    else:
        dbt_type = os.getenv("DBT_TYPE")
        if not dbt_type:
            raise ValueError("DBT_TYPE is required when DBT_TARGET_OUTPUT_JSON is not provided.")

        output = {
            "type": dbt_type,
            "threads": int(os.getenv("DBT_THREADS", "4")),
        }
        schema = os.getenv("DBT_SCHEMA", "public")

        if dbt_type in {"postgres", "redshift"}:
            output.update(
                {
                    "host": os.getenv("DBT_HOST", "host.docker.internal"),
                    "port": int(os.getenv("DBT_PORT", "5432")),
                    "user": os.getenv("DBT_USER"),
                    "pass": os.getenv("DBT_PASSWORD"),
                    "dbname": os.getenv("DBT_DATABASE"),
                    "schema": schema,
                }
            )
            sslmode = os.getenv("DBT_SSLMODE")
            if sslmode:
                output["sslmode"] = sslmode
        elif dbt_type == "snowflake":
            output.update(
                {
                    "account": os.getenv("DBT_ACCOUNT"),
                    "user": os.getenv("DBT_USER"),
                    "password": os.getenv("DBT_PASSWORD"),
                    "database": os.getenv("DBT_DATABASE"),
                    "schema": schema,
                    "warehouse": os.getenv("DBT_WAREHOUSE"),
                }
            )
            role = os.getenv("DBT_ROLE")
            authenticator = os.getenv("DBT_AUTHENTICATOR")
            if role:
                output["role"] = role
            if authenticator:
                output["authenticator"] = authenticator
        elif dbt_type == "bigquery":
            output.update(
                {
                    "project": os.getenv("DBT_PROJECT"),
                    "dataset": schema,
                }
            )
            location = os.getenv("DBT_LOCATION")
            keyfile = os.getenv("DBT_KEYFILE")
            if location:
                output["location"] = location
            if keyfile:
                output["keyfile"] = keyfile
        else:
            if schema:
                output["schema"] = schema

        extra_json = os.getenv("DBT_OUTPUT_EXTRA_JSON")
        if extra_json:
            output.update(json.loads(extra_json))

    if "type" not in output:
        dbt_type = os.getenv("DBT_TYPE")
        if not dbt_type:
            raise ValueError(
                "Profile output is missing 'type'; "
                "set DBT_TYPE or include it in DBT_TARGET_OUTPUT_JSON."
            )
        output["type"] = dbt_type

    return output


def main() -> None:
    output_path = sys.argv[1]
    target_name = os.getenv("DBT_TARGET", "default")
    profile_name = os.getenv("DBT_PROFILE_NAME", "analytics_premium")

    profile = {
        profile_name: {
            "target": target_name,
            "outputs": {
                target_name: build_output(),
            },
        }
    }

    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(profile, fp, indent=2)
        fp.write("\n")


if __name__ == "__main__":
    main()
