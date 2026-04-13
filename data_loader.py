from __future__ import annotations

from pathlib import Path

import pandas as pd

from .utils import clean_sub_label, is_unnamed_label, unique_columns


def load_workbook_sheets(path: Path) -> list[dict]:
    workbook = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    sheets_data = []
    for sheet_name, df in workbook.items():
        prepared_df = df.fillna("")
        sheets_data.append(
            {
                "name": sheet_name,
                "columns": list(prepared_df.columns),
                "rows": prepared_df.to_dict(orient="records"),
                "row_count": len(prepared_df),
            }
        )
    return sheets_data


def build_class_tables_from_services(services: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for service in services:
        class_name = service["class_name"]
        if class_name not in grouped:
            grouped[class_name] = {
                "class_name": class_name,
                "services": [],
                "direct_total": 0.0,
                "indirect_total": 0.0,
                "inefficiency_total": 0.0,
                "indirect_sum_total": 0.0,
                "grand_total": 0.0,
            }

        bucket = grouped[class_name]
        bucket["services"].append(
            {
                "service_name": service["service_name"],
                "direct_cost": service["direct_cost"],
                "indirect_cost": service["indirect_cost"],
                "inefficiency_cost": service["inefficiency_cost"],
                "indirect_sum": service["indirect_sum"],
                "total_cost": service["total_cost"],
            }
        )
        bucket["direct_total"] += service["direct_cost"]
        bucket["indirect_total"] += service["indirect_cost"]
        bucket["inefficiency_total"] += service["inefficiency_cost"]
        bucket["indirect_sum_total"] += service["indirect_sum"]
        bucket["grand_total"] += service["total_cost"]

    return list(grouped.values())


def load_calculation_services_dataset(path: Path) -> dict:
    df = pd.read_excel(path, sheet_name=0, header=[2, 3], engine="openpyxl")
    columns = list(df.columns)
    if len(columns) < 25:
        raise ValueError("Лист 'Калькуляция' имеет неожиданную структуру колонок.")

    number_col = columns[0]
    class_col = columns[1]
    service_col = columns[2]
    direct_total_col = columns[4]
    indirect_total_col = columns[16]
    ineff_cols = [columns[22], columns[23], columns[24]]

    direct_top = clean_sub_label(direct_total_col[0])
    indirect_top = clean_sub_label(indirect_total_col[0])
    ineff_top = clean_sub_label(ineff_cols[0][0])

    direct_columns = [
        col
        for col in columns
        if isinstance(col, tuple) and clean_sub_label(col[0]) == direct_top
    ]
    indirect_columns = [
        col
        for col in columns
        if isinstance(col, tuple) and clean_sub_label(col[0]) == indirect_top
    ]
    ineff_columns = [
        col
        for col in columns
        if isinstance(col, tuple) and clean_sub_label(col[0]) == ineff_top
    ]

    direct_detail_cols = [
        col
        for col in direct_columns
        if col != direct_total_col
        and "%" not in clean_sub_label(col[1])
        and not is_unnamed_label(col[1])
    ]
    indirect_detail_cols = [
        col
        for col in indirect_columns
        if col != indirect_total_col and not is_unnamed_label(col[1])
    ]
    ineff_detail_cols = [col for col in ineff_columns if not is_unnamed_label(col[1])]

    selected_columns = unique_columns([
        number_col,
        class_col,
        service_col,
        direct_total_col,
        indirect_total_col,
        *ineff_cols,
        *direct_detail_cols,
        *indirect_detail_cols,
        *ineff_detail_cols,
    ])
    data = df[selected_columns].copy()
    data = data[pd.to_numeric(data[number_col], errors="coerce").notna()]

    data[class_col] = data[class_col].fillna("Без класса").astype(str).str.strip()
    data[service_col] = data[service_col].fillna("").astype(str).str.strip()
    data = data[data[service_col] != ""]
    data = data[data[service_col].str.casefold() != "обоснование"]

    numeric_columns = unique_columns([
        direct_total_col,
        indirect_total_col,
        *ineff_cols,
        *direct_detail_cols,
        *indirect_detail_cols,
        *ineff_detail_cols,
    ])
    for col in numeric_columns:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    services = []
    for _, row in data.iterrows():
        class_name = str(row[class_col]).strip() or "Без класса"
        service_name = str(row[service_col]).strip()

        direct_total = float(row[direct_total_col])
        indirect_total_with_ineff = float(row[indirect_total_col])
        ineff_total = sum(float(row[col]) for col in ineff_cols)
        indirect_core = indirect_total_with_ineff - ineff_total
        indirect_sum = indirect_core + ineff_total
        total_cost = direct_total + indirect_sum

        direct_details = {
            clean_sub_label(col[1]): float(row[col]) for col in direct_detail_cols
        }
        indirect_details = {
            clean_sub_label(col[1]): float(row[col]) for col in indirect_detail_cols
        }
        inefficiency_details = {
            clean_sub_label(col[1]): float(row[col]) for col in ineff_detail_cols
        }

        services.append(
            {
                "class_name": class_name,
                "service_name": service_name,
                "direct_cost": direct_total,
                "indirect_cost": indirect_core,
                "inefficiency_cost": ineff_total,
                "indirect_sum": indirect_sum,
                "total_cost": total_cost,
                "direct_details": direct_details,
                "indirect_details": indirect_details,
                "inefficiency_details": inefficiency_details,
            }
        )

    class_tables = build_class_tables_from_services(services)
    return {
        "services": services,
        "class_tables": class_tables,
        "direct_detail_labels": [clean_sub_label(col[1]) for col in direct_detail_cols],
        "indirect_detail_labels": [clean_sub_label(col[1]) for col in indirect_detail_cols],
        "inefficiency_detail_labels": [clean_sub_label(col[1]) for col in ineff_detail_cols],
    }


def load_calculation_services_by_class(path: Path) -> list[dict]:
    return load_calculation_services_dataset(path)["class_tables"]