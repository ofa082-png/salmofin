#!/usr/bin/env python3
"""Extract Akvafakta feed statistics from public Power BI querydata."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


QUERY_URL = "https://wabi-north-europe-h-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true"
DATA_DIR = Path(__file__).resolve().parents[1] / "akvafakta_powerbi_data"

REPORTS = {
    "weekly": {
        "resource_key": "7502fffe-8a69-4b8f-8d70-4073acfe6e8c",
        "model_id": 1819799,
        "entity": "Tabell3_2",
        "columns": ["År", "Uke", "Attributt", "Verdi"],
        "csv_headers": ["år", "uke", "attributt", "verdi"],
        "output": DATA_DIR / "forstatistikk_uke.csv",
        "raw_output": DATA_DIR / "forstatistikk_uke_raw_querydata.json",
    },
    "monthly": {
        "resource_key": "a8009d27-b944-4a3d-b9f9-cecdcb747123",
        "model_id": 1819786,
        "entity": "Måned_tall",
        "columns": ["År", "Måned", "Type", "Column5", "Verdi"],
        "csv_headers": ["år", "måned", "type", "column5", "verdi"],
        "output": DATA_DIR / "forstatistikk_maned.csv",
        "raw_output": DATA_DIR / "forstatistikk_maned_raw_querydata.json",
    },
}


def column_ref(alias: str, property_name: str) -> dict[str, Any]:
    return {
        "Column": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property": property_name,
        }
    }


def build_query_payload(entity: str, columns: list[str], row_limit: int, model_id: int) -> dict[str, Any]:
    alias = entity[0].lower()
    select = [
        {
            **column_ref(alias, column),
            "Name": f"{entity}.{column}",
            "NativeReferenceName": column,
        }
        for column in columns
    ]

    command = {
        "Version": 2,
        "From": [{"Name": alias, "Entity": entity, "Type": 0}],
        "Select": select,
        "OrderBy": [
            {"Direction": 1, "Expression": column_ref(alias, column)}
            for column in columns[:2]
        ],
    }

    data_shape = {
        "Query": command,
        "Binding": {
            "Primary": {
                "Groupings": [
                    {
                        "Projections": list(range(len(columns))),
                        "Subtotal": 1,
                    }
                ],
            },
            "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": row_limit}}},
            "Version": 1,
        },
        "ExecutionMetricsKind": 1,
    }

    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {"SemanticQueryDataShapeCommand": data_shape},
                    ]
                },
                "CacheKey": "",
                "QueryId": str(uuid.uuid4()),
                "ApplicationContext": {"DatasetId": str(uuid.uuid4())},
            }
        ],
        "cancelQueries": [],
        "modelId": model_id,
    }


def post_query(resource_key: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request_id = str(uuid.uuid4())
    request = Request(
        QUERY_URL,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "ActivityId": str(uuid.uuid4()),
            "RequestId": request_id,
            "X-PowerBI-ResourceKey": resource_key,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Power BI query failed with HTTP {exc.code}: {details}") from exc
    except URLError as exc:
        raise RuntimeError(f"Power BI query failed: {exc.reason}") from exc


def value_dicts(response: dict[str, Any]) -> dict[str, list[Any]]:
    ds = response["results"][0]["result"]["data"]["dsr"]["DS"][0]
    return ds.get("ValueDicts", {})


def dm0_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    dsr = response["results"][0]["result"]["data"]["dsr"]
    if "DS" not in dsr:
        raise ValueError(f"Power BI response did not include data rows: {json.dumps(dsr, ensure_ascii=False)}")
    return dsr["DS"][0]["PH"][0]["DM0"]


def decode_rows(response: dict[str, Any], expected_columns: int) -> list[list[Any]]:
    rows = dm0_rows(response)
    dicts = value_dicts(response)
    decoded: list[list[Any]] = []
    previous: list[Any] = [None] * expected_columns

    for item in rows:
        current = previous.copy()
        repeated_mask = int(item.get("R", 0))
        null_mask = int(item.get("Ø", 0))
        values = iter(item.get("C", []))

        for index in range(expected_columns):
            bit = 1 << index
            if repeated_mask & bit:
                continue
            if null_mask & bit:
                current[index] = None
                continue
            try:
                current[index] = next(values)
            except StopIteration as exc:
                raise ValueError(f"Compressed row ended early at column {index}: {item}") from exc

        if any(value is not None for value in current):
            decoded.append(expand_value_dicts(current, rows[0].get("S", []), dicts))
        previous = current

    return decoded


def expand_value_dicts(row: list[Any], schema: list[dict[str, Any]], dicts: dict[str, list[Any]]) -> list[Any]:
    expanded = row.copy()
    for index, spec in enumerate(schema):
        dictionary_name = spec.get("DN")
        if dictionary_name is None:
            continue
        dictionary = dicts.get(dictionary_name, [])
        value = expanded[index]
        if value is None:
            continue
        expanded[index] = dictionary[int(value)]
    return expanded


def write_rows_csv(path: Path, headers: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(["" if value is None else value for value in row])


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
        handle.write("\n")


def extract_report(name: str, args: argparse.Namespace) -> int:
    report = REPORTS[name]
    raw_path = Path(args.raw_input) if args.raw_input and len(args.reports) == 1 else report["raw_output"]
    if args.from_raw:
        response = load_json(raw_path)
    else:
        payload = build_query_payload(report["entity"], report["columns"], args.row_limit, report["model_id"])
        response = post_query(report["resource_key"], payload, args.timeout)
        if args.save_raw:
            save_json(report["raw_output"], response)

    rows = decode_rows(response, len(report["columns"]))
    write_rows_csv(report["output"], report["csv_headers"], rows)
    print(f"{name}: wrote {len(rows)} rows to {report['output']}")
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "reports",
        nargs="*",
        choices=sorted(REPORTS),
        help="Reports to extract. Defaults to both.",
    )
    parser.add_argument("--from-raw", action="store_true", help="Decode saved raw querydata JSON instead of POSTing.")
    parser.add_argument("--raw-input", help="Raw querydata JSON path, only valid with a single report.")
    parser.add_argument("--save-raw", action="store_true", help="Save raw querydata JSON next to the CSV outputs.")
    parser.add_argument("--row-limit", type=int, default=100000, help="Power BI row window count.")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds.")
    args = parser.parse_args()
    if args.raw_input and len(args.reports) != 1:
        parser.error("--raw-input requires exactly one report")
    if not args.reports:
        args.reports = sorted(REPORTS)
    return args


def main() -> int:
    args = parse_args()
    try:
        for report in args.reports:
            extract_report(report, args)
    except (KeyError, ValueError, RuntimeError, OSError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
