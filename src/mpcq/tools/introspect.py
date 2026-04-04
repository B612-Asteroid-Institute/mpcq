from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from google.cloud import bigquery

PYARROW_TYPE_MAP = {
    "STRING": "qv.LargeStringColumn",
    "BYTES": "qv.BinaryColumn",
    "INTEGER": "qv.Int64Column",
    "INT64": "qv.Int64Column",
    "FLOAT": "qv.Float64Column",
    "FLOAT64": "qv.Float64Column",
    "BOOLEAN": "qv.BooleanColumn",
    "BOOL": "qv.BooleanColumn",
    # TIMESTAMPs are handled by Timestamp.from_astropy downstream, but for schema
    # we keep them as Timestamp.as_column(nullable=True)
    "TIMESTAMP": "Timestamp.as_column",
    "DATE": "qv.Date32Column",
    "DATETIME": "qv.LargeStringColumn",  # could be specialized if needed
    "TIME": "qv.LargeStringColumn",
    "GEOGRAPHY": "qv.LargeStringColumn",
    "NUMERIC": "qv.Float64Column",
    "BIGNUMERIC": "qv.Float64Column",
}


@dataclass
class ColumnDef:
    name: str
    bq_type: str
    nullable: bool


def _iter_columns(schema: Iterable[bigquery.SchemaField]) -> Iterable[ColumnDef]:
    for field in schema:
        yield ColumnDef(
            name=field.name,
            bq_type=field.field_type.upper(),
            nullable=field.is_nullable,
        )


def _to_quivr_line(column: ColumnDef) -> str:
    import quivr as qv  # noqa: F401  - for reference in generated lines
    from adam_core.time import Timestamp  # noqa: F401

    col_type = PYARROW_TYPE_MAP.get(column.bq_type, "qv.LargeStringColumn")
    if col_type == "Timestamp.as_column":
        return f"    {column.name} = Timestamp.as_column(nullable=True)"
    return f"    {column.name} = {col_type}(nullable=True)"


def emit_quivr_class_for_table(
    client: bigquery.Client,
    full_table_id: str,
    class_name: str,
    extra_fields: list[str] | None = None,
) -> str:
    table = client.get_table(full_table_id)
    lines = [
        "import quivr as qv",
        "from adam_core.time import Timestamp",
        "",
        f"class {class_name}(qv.Table):",
    ]

    # prepend any extra fields first (like requested_provid)
    if extra_fields:
        for field in extra_fields:
            lines.append(f"    {field}")

    for col in _iter_columns(table.schema):
        lines.append(_to_quivr_line(col))

    return "\n".join(lines) + "\n"


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Emit Quivr class from BigQuery table")
    parser.add_argument("full_table_id", help="<project>.<dataset>.<table>")
    parser.add_argument("class_name", help="Name of the Quivr class to emit")
    parser.add_argument(
        "--extra", nargs="*", default=[], help="Extra class field lines to add at top"
    )
    args = parser.parse_args()

    client = bigquery.Client()
    print(emit_quivr_class_for_table(client, args.full_table_id, args.class_name, args.extra))


if __name__ == "__main__":
    main()
