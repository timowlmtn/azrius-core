import os
import json
import argparse
import re
import pandas as pd
from dotenv import load_dotenv
from typing import Optional
from sqlalchemy import create_engine, text
from client.LlamaPromptHandler import LlamaPromptHandler


class LLMColumnCompressor:
    def __init__(
        self,
        model_handler,
        input_columns: list[str],
        output_column: str = "segment",
        prompt_template: Optional[str] = None,
        batch_size: int = 10,
        output_dir: Optional[str] = None,
    ):
        self.model_handler = model_handler
        self.input_columns = input_columns
        self.output_column = output_column
        self.prompt_template = prompt_template
        self.batch_size = batch_size
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True) if output_dir else None

    def generate_prompt(self, batch_df: pd.DataFrame) -> str:
        lines = [
            f"- item_number: {row['item_number']}, "
            + ", ".join(
                [
                    f"{col.title().replace('_', ' ')}: {row[col]}"
                    for col in self.input_columns
                ]
            )
            for _, row in batch_df.iterrows()
        ]

        return f"""You are an expert in classifying liquor products.
Using the item_number, category, and product description, assign a subcategory (like "vodka", "scotch", "bourbon", "flavored whiskey", "tequila", "rum", etc.).

Return your response in the format:
"item_number" => "subcategory"

Now classify these items and return the results in JSON format as a map with the format {{"000s": "subcategory"}}, do not include any other text or comments:
{chr(10).join(lines)}"""

    def parse_response(self, response_text: str) -> dict:
        print(f"🧠 Raw LLM Response:\n{response_text}")
        try:
            cleaned = re.sub(r"\s*//.*?$", "", response_text, flags=re.MULTILINE)
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            print(f"⚠️ Failed to parse LLM response as JSON: {e}")
            return {}

    def compress(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        total_batches = (len(df) + self.batch_size - 1) // self.batch_size
        print(
            f"🔁 Starting compression: {len(df)} records, batch size = {self.batch_size}, total batches = {total_batches}"
        )

        for i in range(0, len(df), self.batch_size):
            batch_num = (i // self.batch_size) + 1
            batch_df = df.iloc[i : i + self.batch_size]
            print(
                f"\n📦 Batch {batch_num}/{total_batches} - Processing item_numbers: {batch_df['item_number'].tolist()}"
            )

            prompt = self.generate_prompt(batch_df)
            response = self.model_handler.send_prompt(prompt)
            mapping = self.parse_response(response)

            print(f"✅ Batch {batch_num} response mapped {len(mapping)} entries")

            batch_result = batch_df.copy()
            batch_result[self.output_column] = batch_result["item_number"].map(mapping)
            results.append(batch_result)

            if self.output_dir:
                output_file = os.path.join(
                    self.output_dir, f"segment_batch_{batch_num}.json"
                )
                batch_result.to_json(output_file, orient="records", indent=2)
                print(f"📝 Batch {batch_num} written to {output_file}")

        final_df = pd.concat(results, ignore_index=True)
        print(f"\n🏁 Compression complete. Total records: {len(final_df)}")
        return final_df


def main():
    parser = argparse.ArgumentParser(
        description="Compress categorical fields using LLM"
    )

    parser.add_argument("--schema", default="lincoln_liquors")
    parser.add_argument("--source_table", default="iowa_liquor_sales")
    parser.add_argument("--target_table", default="item_dim")
    parser.add_argument(
        "--columns", nargs="+", default=["category_name", "item_description"]
    )
    parser.add_argument("--output_column", default="segment")
    parser.add_argument(
        "--limit", type=int, default=-1, help="Set -1 for unlimited rows"
    )
    parser.add_argument("--batch_size", type=int, default=10, help="LLM batch size")

    args = parser.parse_args()

    # Load environment and connect to DB
    load_dotenv()
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB")

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Prepare SQL query
    col_str = ", ".join(["item_number"] + args.columns)
    limit_clause = f"LIMIT {args.limit}" if args.limit != -1 else ""
    query = f"""
        SELECT DISTINCT s.item_number, s.item_description, s.category_name
        FROM lincoln_liquors.iowa_liquor_sales s
        LEFT JOIN lincoln_liquors.item_dim d
          ON s.item_number = d.item_number
        WHERE d.item_number IS NULL OR d.segment IS NULL
        {limit_clause}
    """
    print(f"📥 Executing query:\n{query}")
    df = pd.read_sql(query, engine)
    print(f"📊 Loaded {len(df)} rows from source table.")

    # Compress
    handler = LlamaPromptHandler("llama3")
    compressor = LLMColumnCompressor(
        model_handler=handler,
        input_columns=args.columns,
        output_column=args.output_column,
        batch_size=args.batch_size,
    )

    df_with_segments = compressor.compress(df)

    # Write to target dimension table
    item_dim_table = f"{args.schema}.{args.target_table}"
    temp_table = "_temp_item_dim"

    with engine.begin() as conn:
        print(f"\n🧱 Ensuring {item_dim_table} exists...")
        conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {item_dim_table} (
                item_number VARCHAR(50) PRIMARY KEY,
                item_description TEXT,
                category_name TEXT,
                {args.output_column} TEXT
            )
        """
            )
        )

        print(f"📤 Writing temp table: {temp_table}")
        df_with_segments.to_sql(
            temp_table,
            con=conn,
            index=False,
            if_exists="replace",
            schema=args.schema,
        )

        print(f"🧹 Deleting existing records from {item_dim_table}...")
        conn.execute(
            text(
                f"""
            DELETE FROM {item_dim_table}
            WHERE item_number IN (
                SELECT item_number FROM {args.schema}.{temp_table}
            );
        """
            )
        )

        print(f"➕ Inserting updated records into {item_dim_table}...")
        conn.execute(
            text(
                f"""
            INSERT INTO {item_dim_table} (
                item_number, item_description, category_name, {args.output_column}
            )
            SELECT
                item_number,
                MIN(item_description),
                MIN(category_name),
                MAX({args.output_column}) FILTER (WHERE {args.output_column} IS NOT NULL)
            FROM {args.schema}.{temp_table}
            GROUP BY item_number;
        """
            )
        )

        print(f"🗑️ Dropping temp table {temp_table}")
        conn.execute(text(f"DROP TABLE {args.schema}.{temp_table}"))

    print(f"\n✅ Segment values written to {item_dim_table}")


if __name__ == "__main__":
    main()
