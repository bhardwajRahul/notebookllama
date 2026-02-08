import pandas as pd
from src.notebookllama.instrumentation import OtelTracesSqlEngine


def test_to_parquet_creates_file(tmp_path):
    # Use in-memory SQLite
    engine = OtelTracesSqlEngine(engine_url="sqlite:///:memory:")

    # Connect and create sample data
    engine._connect()

    df = pd.DataFrame(
        {
            "trace_id": ["abc"],
            "span_id": ["span1"],
            "parent_span_id": [None],
            "operation_name": ["op"],
            "start_time": [123456789],
            "duration": [100],
            "status_code": ["OK"],
            "service_name": ["svc"],
        }
    )

    # Write to SQL
    engine._to_sql(df, if_exists_policy="replace")

    # Export parquet
    output_file = tmp_path / "traces.parquet"
    engine.to_parquet(str(output_file))

    assert output_file.exists()
