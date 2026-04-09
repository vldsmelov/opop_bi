from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import pandas as pd
from flask import Flask, abort, render_template, request, url_for
from werkzeug.exceptions import HTTPException


BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = BASE_DIR / "data" / "data.xlsx"

app = Flask(__name__)


@app.template_filter("money")
def money(value: float) -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        amount = 0.0
    return f"{amount:,.2f}".replace(",", " ").replace(".", ",")


@app.template_filter("pct")
def pct(value: float) -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        amount = 0.0
    return f"{amount:.1f}%".replace(".", ",")


@app.template_filter("urlquote")
def urlquote(value: object) -> str:
    return quote(str(value or ""), safe="")


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


def clean_sub_label(value: object) -> str:
    text = str(value or "")
    return " ".join(text.replace("\n", " ").split())


def is_unnamed_label(value: object) -> bool:
    return clean_sub_label(value).casefold().startswith("unnamed:")


def unique_columns(columns: list[tuple]) -> list[tuple]:
    seen = set()
    result = []
    for col in columns:
        if col in seen:
            continue
        seen.add(col)
        result.append(col)
    return result


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


def build_histogram(values: list[float], bins: int = 8) -> dict:
    if not values:
        return {"labels": [], "values": []}

    min_value = min(values)
    max_value = max(values)
    if max_value == min_value:
        return {"labels": [f"{min_value:,.0f}".replace(",", " ")], "values": [len(values)]}

    step = (max_value - min_value) / bins
    edges = [min_value + i * step for i in range(bins + 1)]
    counts = [0 for _ in range(bins)]
    for value in values:
        idx = int((value - min_value) / step)
        if idx >= bins:
            idx = bins - 1
        counts[idx] += 1

    labels = []
    for idx in range(bins):
        left = f"{edges[idx]:,.0f}".replace(",", " ")
        right = f"{edges[idx + 1]:,.0f}".replace(",", " ")
        labels.append(f"{left}-{right}")

    return {"labels": labels, "values": counts}


def build_dashboard_data(dataset: dict) -> dict:
    class_tables = dataset["class_tables"]
    services_full = dataset["services"]

    total_services = sum(len(group["services"]) for group in class_tables)
    total_classes = len(class_tables)
    direct_total = sum(group["direct_total"] for group in class_tables)
    indirect_total = sum(group["indirect_total"] for group in class_tables)
    inefficiency_total = sum(group["inefficiency_total"] for group in class_tables)
    indirect_sum_total = sum(group["indirect_sum_total"] for group in class_tables)
    grand_total = sum(group["grand_total"] for group in class_tables)

    direct_share_of_total = (direct_total / grand_total * 100.0) if grand_total else 0.0
    indirect_share_of_total = (indirect_total / grand_total * 100.0) if grand_total else 0.0
    inefficiency_share_of_total = (
        (inefficiency_total / grand_total * 100.0) if grand_total else 0.0
    )
    inefficiency_share_of_indirect_sum = (
        (inefficiency_total / indirect_sum_total * 100.0) if indirect_sum_total else 0.0
    )
    direct_avg_per_service = (direct_total / total_services) if total_services else 0.0
    indirect_avg_per_service = (indirect_total / total_services) if total_services else 0.0
    inefficiency_avg_per_service = (
        (inefficiency_total / total_services) if total_services else 0.0
    )

    classes = []
    all_services = []
    for group in class_tables:
        share = (group["grand_total"] / grand_total * 100.0) if grand_total else 0.0
        top_service = max(group["services"], key=lambda service: service["total_cost"])
        classes.append(
            {
                "name": group["class_name"],
                "services_count": len(group["services"]),
                "direct_total": group["direct_total"],
                "indirect_total": group["indirect_total"],
                "inefficiency_total": group["inefficiency_total"],
                "indirect_sum_total": group["indirect_sum_total"],
                "grand_total": group["grand_total"],
                "share": share,
                "top_service_name": top_service["service_name"],
                "top_service_total": top_service["total_cost"],
            }
        )
        for service in group["services"]:
            all_services.append(
                {
                    "class_name": group["class_name"],
                    "service_name": service["service_name"],
                    "direct_cost": service["direct_cost"],
                    "indirect_cost": service["indirect_cost"],
                    "inefficiency_cost": service["inefficiency_cost"],
                    "indirect_sum": service["indirect_sum"],
                    "total_cost": service["total_cost"],
                }
            )

    full_lookup = {
        (row["class_name"], row["service_name"]): row for row in services_full
    }
    for service in all_services:
        source = full_lookup.get((service["class_name"], service["service_name"]))
        if not source:
            continue
        service["direct_details"] = source["direct_details"]
        service["indirect_details"] = source["indirect_details"]
        service["inefficiency_details"] = source["inefficiency_details"]

    top_services = sorted(all_services, key=lambda s: s["total_cost"], reverse=True)[:12]
    top_service_names = [service["service_name"] for service in top_services]
    top_service_totals = [service["total_cost"] for service in top_services]
    class_labels = [class_item["name"] for class_item in classes]
    class_totals = [class_item["grand_total"] for class_item in classes]
    class_direct_values = [class_item["direct_total"] for class_item in classes]
    class_indirect_values = [class_item["indirect_total"] for class_item in classes]
    class_ineff_values = [class_item["inefficiency_total"] for class_item in classes]

    risk_level = "низкая"
    if inefficiency_share_of_total >= 12:
        risk_level = "высокая"
    elif inefficiency_share_of_total >= 8:
        risk_level = "средняя"

    largest_class_name = "-"
    largest_class_share = 0.0
    if classes:
        largest_class = max(classes, key=lambda item: item["grand_total"])
        largest_class_name = largest_class["name"]
        largest_class_share = largest_class["share"]

    largest_service_name = "-"
    largest_service_total = 0.0
    if top_services:
        largest_service = max(top_services, key=lambda item: item["total_cost"])
        largest_service_name = largest_service["service_name"]
        largest_service_total = largest_service["total_cost"]

    classes_sorted = sorted(classes, key=lambda item: item["grand_total"], reverse=True)
    for class_item in classes_sorted:
        total = class_item["grand_total"] or 0.0
        class_item["direct_share"] = (
            class_item["direct_total"] / total * 100.0 if total else 0.0
        )
        class_item["indirect_share"] = (
            class_item["indirect_total"] / total * 100.0 if total else 0.0
        )
        class_item["ineff_share"] = (
            class_item["inefficiency_total"] / total * 100.0 if total else 0.0
        )
        class_item["avg_service_total"] = (
            class_item["grand_total"] / class_item["services_count"]
            if class_item["services_count"]
            else 0.0
        )

    services_sorted_by_total = sorted(all_services, key=lambda item: item["total_cost"], reverse=True)
    services_sorted_by_ineff = sorted(
        all_services, key=lambda item: item["inefficiency_cost"], reverse=True
    )
    service_totals = [service["total_cost"] for service in all_services]
    service_hist = build_histogram(service_totals, bins=8)
    service_median_total = float(pd.Series(service_totals).median()) if service_totals else 0.0
    service_p90_total = float(pd.Series(service_totals).quantile(0.9)) if service_totals else 0.0
    top_service_share = (
        (largest_service_total / grand_total * 100.0) if grand_total else 0.0
    )
    high_ineff_services_count = sum(
        1
        for service in all_services
        if service["total_cost"] and (service["inefficiency_cost"] / service["total_cost"]) >= 0.15
    )

    direct_components = defaultdict(float)
    indirect_components = defaultdict(float)
    ineff_components = defaultdict(float)
    for service in all_services:
        for name, value in service.get("direct_details", {}).items():
            direct_components[name] += float(value)
        for name, value in service.get("indirect_details", {}).items():
            indirect_components[name] += float(value)
        for name, value in service.get("inefficiency_details", {}).items():
            ineff_components[name] += float(value)

    component_rows = []
    for name, amount in direct_components.items():
        component_rows.append({"component": name, "type": "Прямые", "total": amount})
    for name, amount in indirect_components.items():
        component_rows.append({"component": name, "type": "Косвенные", "total": amount})
    for name, amount in ineff_components.items():
        component_rows.append({"component": name, "type": "Неэффективность", "total": amount})
    component_rows.sort(key=lambda item: item["total"], reverse=True)
    for row in component_rows:
        row["share_of_total"] = (row["total"] / grand_total * 100.0) if grand_total else 0.0

    type_totals = {
        "Прямые": sum(direct_components.values()),
        "Косвенные": sum(indirect_components.values()),
        "Неэффективность": sum(ineff_components.values()),
    }

    type_breakdowns = {}
    type_defs = [
        ("direct", "Прямые", "direct_cost"),
        ("indirect", "Косвенные", "indirect_cost"),
        ("ineff", "Неэффективность", "inefficiency_cost"),
    ]
    for key, title, service_key in type_defs:
        components = [row for row in component_rows if row["type"] == title]
        type_total = type_totals[title]
        for row in components:
            row["share_in_type"] = (row["total"] / type_total * 100.0) if type_total else 0.0

        class_values = []
        for class_item in classes_sorted:
            class_value = {
                "Прямые": class_item["direct_total"],
                "Косвенные": class_item["indirect_total"],
                "Неэффективность": class_item["inefficiency_total"],
            }[title]
            class_values.append({"class_name": class_item["name"], "value": class_value})

        top_services_for_type = sorted(
            (
                {
                    "class_name": service["class_name"],
                    "service_name": service["service_name"],
                    "value": service[service_key],
                }
                for service in all_services
            ),
            key=lambda item: item["value"],
            reverse=True,
        )

        type_breakdowns[key] = {
            "title": title,
            "total": type_total,
            "components": components,
            "component_labels": [item["component"] for item in components[:10]],
            "component_values": [item["total"] for item in components[:10]],
            "class_labels": [item["class_name"] for item in class_values],
            "class_values": [item["value"] for item in class_values],
            "top_services": [item for item in top_services_for_type if item["value"] > 0][:10],
        }

    return {
        "totals": {
            "total_services": total_services,
            "total_classes": total_classes,
            "direct_total": direct_total,
            "indirect_total": indirect_total,
            "inefficiency_total": inefficiency_total,
            "indirect_sum_total": indirect_sum_total,
            "grand_total": grand_total,
            "direct_share_of_total": direct_share_of_total,
            "indirect_share_of_total": indirect_share_of_total,
            "inefficiency_share_of_total": inefficiency_share_of_total,
            "inefficiency_share_of_indirect_sum": inefficiency_share_of_indirect_sum,
            "direct_avg_per_service": direct_avg_per_service,
            "indirect_avg_per_service": indirect_avg_per_service,
            "inefficiency_avg_per_service": inefficiency_avg_per_service,
            "risk_level": risk_level,
            "largest_class_name": largest_class_name,
            "largest_class_share": largest_class_share,
            "largest_service_name": largest_service_name,
            "largest_service_total": largest_service_total,
        },
        "charts": {
            "cost_structure_labels": ["Прямые расходы", "Косвенные расходы", "Неэффективность"],
            "cost_structure_values": [direct_total, indirect_total, inefficiency_total],
            "class_labels": class_labels,
            "class_totals": class_totals,
            "class_direct_values": class_direct_values,
            "class_indirect_values": class_indirect_values,
            "class_ineff_values": class_ineff_values,
            "top_service_labels": top_service_names,
            "top_service_values": top_service_totals,
        },
        "all_services": all_services,
        "classes": classes,
        "top_services": top_services,
        "class_tables": class_tables,
        "level2": {
            "classes_sorted": classes_sorted,
            "class_labels": [item["name"] for item in classes_sorted],
            "class_totals": [item["grand_total"] for item in classes_sorted],
            "class_ineff_shares": [item["ineff_share"] for item in classes_sorted],
            "top2_share": sum(item["share"] for item in classes_sorted[:2]) if classes_sorted else 0.0,
            "avg_class_total": (grand_total / total_classes) if total_classes else 0.0,
        },
        "level3": {
            "service_count": len(all_services),
            "service_median_total": service_median_total,
            "service_p90_total": service_p90_total,
            "top_service_share": top_service_share,
            "high_ineff_services_count": high_ineff_services_count,
            "top_total_labels": [item["service_name"] for item in services_sorted_by_total[:15]],
            "top_total_values": [item["total_cost"] for item in services_sorted_by_total[:15]],
            "top_ineff_labels": [item["service_name"] for item in services_sorted_by_ineff[:15]],
            "top_ineff_values": [item["inefficiency_cost"] for item in services_sorted_by_ineff[:15]],
            "hist_labels": service_hist["labels"],
            "hist_values": service_hist["values"],
            "services_sorted": services_sorted_by_total,
        },
        "level4": {
            "component_rows": component_rows,
            "components_count": len(component_rows),
            "top_component_name": component_rows[0]["component"] if component_rows else "-",
            "top_component_value": component_rows[0]["total"] if component_rows else 0.0,
            "component_labels": [item["component"] for item in component_rows[:12]],
            "component_values": [item["total"] for item in component_rows[:12]],
            "type_labels": list(type_totals.keys()),
            "type_values": list(type_totals.values()),
        },
        "level5": type_breakdowns,
    }


COST_TYPE_META = {
    "direct": {
        "title": "Прямые расходы",
        "field": "direct_cost",
        "details_field": "direct_details",
        "color": "#1f6f66",
    },
    "indirect": {
        "title": "Косвенные расходы",
        "field": "indirect_cost",
        "details_field": "indirect_details",
        "color": "#3e7cb1",
    },
    "ineff": {
        "title": "Неэффективность",
        "field": "inefficiency_cost",
        "details_field": "inefficiency_details",
        "color": "#d47d2f",
    },
}


def calc_totals(services: list[dict]) -> dict:
    return {
        "direct": sum(float(service.get("direct_cost", 0.0)) for service in services),
        "indirect": sum(float(service.get("indirect_cost", 0.0)) for service in services),
        "ineff": sum(float(service.get("inefficiency_cost", 0.0)) for service in services),
        "total": sum(float(service.get("total_cost", 0.0)) for service in services),
    }


def get_cost_meta_or_404(cost_key: str) -> dict:
    meta = COST_TYPE_META.get(cost_key)
    if not meta:
        abort(404, description=f"Тип затрат не найден: {cost_key}")
    return meta


def build_cost_scope_data(services: list[dict], cost_key: str) -> dict:
    meta = get_cost_meta_or_404(cost_key)
    field = meta["field"]
    details_field = meta["details_field"]

    totals = calc_totals(services)
    selected_total = sum(float(service.get(field, 0.0)) for service in services)

    class_buckets: dict[str, dict] = {}
    service_rows: list[dict] = []
    component_totals: defaultdict[str, float] = defaultdict(float)

    for service in services:
        class_name = str(service.get("class_name", "")).strip() or "Без класса"
        service_name = str(service.get("service_name", "")).strip()
        service_total = float(service.get("total_cost", 0.0))
        selected_value = float(service.get(field, 0.0))

        service_rows.append(
            {
                "class_name": class_name,
                "service_name": service_name,
                "value": selected_value,
                "service_total": service_total,
                "share_in_service": (selected_value / service_total * 100.0) if service_total else 0.0,
                "share_in_scope": (selected_value / selected_total * 100.0) if selected_total else 0.0,
            }
        )

        bucket = class_buckets.setdefault(
            class_name,
            {
                "class_name": class_name,
                "services_count": 0,
                "value": 0.0,
                "class_total": 0.0,
            },
        )
        bucket["services_count"] += 1
        bucket["value"] += selected_value
        bucket["class_total"] += service_total

        for component, value in (service.get(details_field, {}) or {}).items():
            component_totals[str(component)] += float(value)

    service_rows.sort(key=lambda row: (row["value"], row["service_total"]), reverse=True)

    class_rows = list(class_buckets.values())
    class_rows.sort(key=lambda row: (row["value"], row["class_total"]), reverse=True)
    for row in class_rows:
        row["share_in_scope"] = (row["value"] / selected_total * 100.0) if selected_total else 0.0
        row["share_in_class"] = (
            (row["value"] / row["class_total"] * 100.0) if row["class_total"] else 0.0
        )

    component_rows = [
        {
            "component": name,
            "value": amount,
            "share_in_scope": (amount / selected_total * 100.0) if selected_total else 0.0,
        }
        for name, amount in component_totals.items()
    ]
    component_rows.sort(key=lambda row: row["value"], reverse=True)

    top_class_name = class_rows[0]["class_name"] if class_rows else "-"
    top_class_value = class_rows[0]["value"] if class_rows else 0.0
    top_service_name = service_rows[0]["service_name"] if service_rows else "-"
    top_service_value = service_rows[0]["value"] if service_rows else 0.0

    return {
        "cost_key": cost_key,
        "meta": meta,
        "scope_total": totals["total"],
        "selected_total": selected_total,
        "share_of_scope": (selected_total / totals["total"] * 100.0) if totals["total"] else 0.0,
        "service_rows": service_rows,
        "class_rows": class_rows,
        "component_rows": component_rows,
        "top_class_name": top_class_name,
        "top_class_value": top_class_value,
        "top_service_name": top_service_name,
        "top_service_value": top_service_value,
    }


def build_navigation_data(dataset: dict) -> dict:
    services = list(dataset.get("services", []))
    services = [service for service in services if service.get("service_name")]
    services.sort(key=lambda service: float(service.get("total_cost", 0.0)), reverse=True)

    totals = calc_totals(services)
    class_map: dict[str, list[dict]] = defaultdict(list)
    for service in services:
        class_map[str(service["class_name"])].append(service)

    class_rows: list[dict] = []
    service_index: dict[tuple[str, str], dict] = {}
    for class_name, class_services in class_map.items():
        class_services.sort(key=lambda service: float(service.get("total_cost", 0.0)), reverse=True)
        class_totals = calc_totals(class_services)
        share = (class_totals["total"] / totals["total"] * 100.0) if totals["total"] else 0.0
        class_rows.append(
            {
                "name": class_name,
                "services_count": len(class_services),
                "totals": class_totals,
                "share": share,
                "top_service_name": class_services[0]["service_name"] if class_services else "-",
                "services": class_services,
            }
        )
        for service in class_services:
            service_index[(class_name, service["service_name"])] = service

    class_rows.sort(key=lambda row: row["totals"]["total"], reverse=True)
    class_index = {row["name"]: row for row in class_rows}

    service_picker = [
        {
            "label": f"{service['service_name']} - {service['class_name']}",
            "class_name": service["class_name"],
            "service_name": service["service_name"],
        }
        for service in services
    ]

    return {
        "services": services,
        "totals": totals,
        "class_rows": class_rows,
        "class_index": class_index,
        "service_index": service_index,
        "service_picker": service_picker,
    }


def load_dashboard_payload(path: Path) -> tuple[dict, dict, dict]:
    dataset = load_calculation_services_dataset(path)
    overview = build_dashboard_data(dataset)
    navigation = build_navigation_data(dataset)
    return dataset, overview, navigation


def get_class_or_404(navigation: dict, class_name: str) -> dict:
    class_row = navigation["class_index"].get(class_name)
    if not class_row:
        abort(404, description=f"Класс не найден: {class_name}")
    return class_row


def get_service_or_404(navigation: dict, class_name: str, service_name: str) -> dict:
    service = navigation["service_index"].get((class_name, service_name))
    if not service:
        abort(404, description=f"Услуга не найдена: {service_name}")
    return service


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/excel-debug")
def excel_debug():
    if not EXCEL_PATH.exists():
        return render_template("excel_debug.html", error=f"Файл не найден: {EXCEL_PATH}", sheets=[])

    sheets = load_workbook_sheets(EXCEL_PATH)
    return render_template("excel_debug.html", error=None, sheets=sheets)


@app.route("/calculation-services-debug")
def calculation_services_debug():
    if not EXCEL_PATH.exists():
        return render_template(
            "calculation_services_debug.html",
            error=f"Файл не найден: {EXCEL_PATH}",
            class_tables=[],
        )

    try:
        class_tables = load_calculation_services_by_class(EXCEL_PATH)
        return render_template(
            "calculation_services_debug.html",
            error=None,
            class_tables=class_tables,
        )
    except Exception as exc:
        return render_template(
            "calculation_services_debug.html",
            error=f"Ошибка обработки листа 'Калькуляция': {exc}",
            class_tables=[],
        )


def render_dashboard_error(error: str, status_code: int = 500):
    return (
        render_template(
            "dashboard_error.html",
            error=error,
            current_level=1,
            current_class_name=None,
            current_service_name=None,
            current_cost_title=None,
            current_cost_key=None,
            navigation_mode="class",
            breadcrumbs=[{"name": "Главная", "href": url_for("dashboard_home")}],
        ),
        status_code,
    )


@app.errorhandler(404)
def handle_404(error):
    if request.path.startswith("/dashboard"):
        description = getattr(error, "description", None) or "Страница не найдена"
        return render_dashboard_error(description, status_code=404)
    return render_template("dashboard_error.html", error="Страница не найдена"), 404


@app.route("/dashboard")
def dashboard_home():
    if not EXCEL_PATH.exists():
        return render_dashboard_error(f"Файл не найден: {EXCEL_PATH}", status_code=404)

    try:
        _, overview, navigation = load_dashboard_payload(EXCEL_PATH)
        service_picker = [
            {
                "label": item["label"],
                "href": url_for(
                    "dashboard_service",
                    class_name=item["class_name"],
                    service_name=item["service_name"],
                ),
            }
            for item in navigation["service_picker"]
        ]

        return render_template(
            "dashboard_home.html",
            overview=overview,
            navigation=navigation,
            service_picker=service_picker,
            cost_drill_links={
                key: url_for("dashboard_cost_overview", cost_key=key)
                for key in COST_TYPE_META
            },
            current_level=1,
            current_class_name=None,
            current_service_name=None,
            current_cost_title=None,
            current_cost_key=None,
            navigation_mode="class",
            breadcrumbs=[{"name": "Главная", "href": url_for("dashboard_home")}],
        )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        return render_dashboard_error(f"Ошибка построения дашборда: {exc}")


@app.route("/dashboard/cost/<cost_key>")
def dashboard_cost_overview(cost_key: str):
    if not EXCEL_PATH.exists():
        return render_dashboard_error(f"Файл не найден: {EXCEL_PATH}", status_code=404)

    try:
        _, overview, navigation = load_dashboard_payload(EXCEL_PATH)
        cost_scope = build_cost_scope_data(navigation["services"], cost_key)
        cost_meta = cost_scope["meta"]

        for row in cost_scope["class_rows"]:
            row["href"] = url_for(
                "dashboard_cost_class",
                cost_key=cost_key,
                class_name=row["class_name"],
            )

        for row in cost_scope["service_rows"]:
            row["class_href"] = url_for(
                "dashboard_cost_class",
                cost_key=cost_key,
                class_name=row["class_name"],
            )
            row["service_href"] = url_for(
                "dashboard_cost_detail",
                class_name=row["class_name"],
                service_name=row["service_name"],
                cost_key=cost_key,
                source="cost",
            )

        breadcrumbs = [
            {"name": "Главная", "href": url_for("dashboard_home")},
            {
                "name": cost_meta["title"],
                "href": url_for("dashboard_cost_overview", cost_key=cost_key),
            },
        ]

        return render_template(
            "dashboard_cost_overview.html",
            overview=overview,
            navigation=navigation,
            cost_scope=cost_scope,
            cost_meta=cost_meta,
            current_level=2,
            current_class_name=None,
            current_service_name=None,
            current_cost_title=cost_meta["title"],
            current_cost_key=cost_key,
            navigation_mode="cost",
            breadcrumbs=breadcrumbs,
        )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        return render_dashboard_error(f"Ошибка построения среза затрат: {exc}")


@app.route("/dashboard/cost/<cost_key>/class/<class_name>")
def dashboard_cost_class(cost_key: str, class_name: str):
    if not EXCEL_PATH.exists():
        return render_dashboard_error(f"Файл не найден: {EXCEL_PATH}", status_code=404)

    try:
        _, overview, navigation = load_dashboard_payload(EXCEL_PATH)
        class_row = get_class_or_404(navigation, class_name)
        cost_scope = build_cost_scope_data(class_row["services"], cost_key)
        global_scope = build_cost_scope_data(navigation["services"], cost_key)
        cost_meta = cost_scope["meta"]

        global_classes = global_scope["class_rows"]
        class_rank = next(
            (idx + 1 for idx, row in enumerate(global_classes) if row["class_name"] == class_name),
            0,
        )
        class_share_global = next(
            (row.get("share_in_scope", 0.0) for row in global_classes if row["class_name"] == class_name),
            0.0,
        )

        for row in cost_scope["service_rows"]:
            row["service_href"] = url_for(
                "dashboard_cost_detail",
                class_name=class_name,
                service_name=row["service_name"],
                cost_key=cost_key,
                source="cost",
            )

        breadcrumbs = [
            {"name": "Главная", "href": url_for("dashboard_home")},
            {
                "name": cost_meta["title"],
                "href": url_for("dashboard_cost_overview", cost_key=cost_key),
            },
            {
                "name": class_name,
                "href": url_for(
                    "dashboard_cost_class",
                    cost_key=cost_key,
                    class_name=class_name,
                ),
            },
        ]

        return render_template(
            "dashboard_cost_class.html",
            overview=overview,
            navigation=navigation,
            class_row=class_row,
            cost_scope=cost_scope,
            cost_meta=cost_meta,
            class_rank=class_rank,
            class_share_global=class_share_global,
            current_level=3,
            current_class_name=class_name,
            current_service_name=None,
            current_cost_title=cost_meta["title"],
            current_cost_key=cost_key,
            navigation_mode="cost",
            breadcrumbs=breadcrumbs,
        )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        return render_dashboard_error(f"Ошибка построения среза класса: {exc}")


@app.route("/dashboard/class/<class_name>")
def dashboard_class(class_name: str):
    if not EXCEL_PATH.exists():
        return render_dashboard_error(f"Файл не найден: {EXCEL_PATH}", status_code=404)

    try:
        _, overview, navigation = load_dashboard_payload(EXCEL_PATH)
        class_row = get_class_or_404(navigation, class_name)
        services = class_row["services"]
        service_labels = [service["service_name"] for service in services]
        service_totals = [service["total_cost"] for service in services]
        cost_structure = [
            class_row["totals"]["direct"],
            class_row["totals"]["indirect"],
            class_row["totals"]["ineff"],
        ]

        breadcrumbs = [
            {"name": "Главная", "href": url_for("dashboard_home")},
            {
                "name": class_name,
                "href": url_for("dashboard_class", class_name=class_name),
            },
        ]

        return render_template(
            "dashboard_class.html",
            overview=overview,
            navigation=navigation,
            class_row=class_row,
            service_labels=service_labels,
            service_totals=service_totals,
            cost_structure=cost_structure,
            class_cost_links={
                key: url_for("dashboard_cost_class", cost_key=key, class_name=class_name)
                for key in COST_TYPE_META
            },
            current_level=2,
            current_class_name=class_name,
            current_service_name=None,
            current_cost_title=None,
            current_cost_key=None,
            navigation_mode="class",
            breadcrumbs=breadcrumbs,
        )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        return render_dashboard_error(f"Ошибка построения страницы класса: {exc}")


@app.route("/dashboard/class/<class_name>/service/<service_name>")
def dashboard_service(class_name: str, service_name: str):
    if not EXCEL_PATH.exists():
        return render_dashboard_error(f"Файл не найден: {EXCEL_PATH}", status_code=404)

    try:
        _, overview, navigation = load_dashboard_payload(EXCEL_PATH)
        class_row = get_class_or_404(navigation, class_name)
        service = get_service_or_404(navigation, class_name, service_name)

        total_cost = float(service["total_cost"])
        cost_cards = []
        for cost_key, meta in COST_TYPE_META.items():
            amount = float(service[meta["field"]])
            share = (amount / total_cost * 100.0) if total_cost else 0.0
            cost_cards.append(
                {
                    "key": cost_key,
                    "title": meta["title"],
                    "amount": amount,
                    "share": share,
                    "color": meta["color"],
                    "href": url_for(
                        "dashboard_cost_detail",
                        class_name=class_name,
                        service_name=service_name,
                        cost_key=cost_key,
                    ),
                }
            )

        peer_services = class_row["services"]
        peer_labels = [item["service_name"] for item in peer_services]
        peer_totals = [item["total_cost"] for item in peer_services]
        component_rows = []
        for _, meta in COST_TYPE_META.items():
            details = service.get(meta["details_field"], {}) or {}
            for component, value in details.items():
                value_float = float(value)
                if value_float == 0:
                    continue
                component_rows.append(
                    {
                        "type": meta["title"],
                        "component": component,
                        "value": value_float,
                    }
                )
        component_rows.sort(key=lambda row: row["value"], reverse=True)

        breadcrumbs = [
            {"name": "Главная", "href": url_for("dashboard_home")},
            {
                "name": class_name,
                "href": url_for("dashboard_class", class_name=class_name),
            },
            {
                "name": service_name,
                "href": url_for(
                    "dashboard_service",
                    class_name=class_name,
                    service_name=service_name,
                ),
            },
        ]

        return render_template(
            "dashboard_service.html",
            overview=overview,
            navigation=navigation,
            class_row=class_row,
            service=service,
            cost_cards=cost_cards,
            peer_labels=peer_labels,
            peer_totals=peer_totals,
            component_rows=component_rows[:25],
            current_level=3,
            current_class_name=class_name,
            current_service_name=service_name,
            current_cost_title=None,
            current_cost_key=None,
            navigation_mode="class",
            breadcrumbs=breadcrumbs,
        )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        return render_dashboard_error(f"Ошибка построения страницы услуги: {exc}")


@app.route("/dashboard/class/<class_name>/service/<service_name>/cost/<cost_key>")
def dashboard_cost_detail(class_name: str, service_name: str, cost_key: str):
    if not EXCEL_PATH.exists():
        return render_dashboard_error(f"Файл не найден: {EXCEL_PATH}", status_code=404)
    if cost_key not in COST_TYPE_META:
        abort(404, description=f"Тип затрат не найден: {cost_key}")

    try:
        _, overview, navigation = load_dashboard_payload(EXCEL_PATH)
        class_row = get_class_or_404(navigation, class_name)
        service = get_service_or_404(navigation, class_name, service_name)
        meta = get_cost_meta_or_404(cost_key)
        source = request.args.get("source", "").strip().casefold()

        selected_value = float(service.get(meta["field"], 0.0))
        selected_share = (
            (selected_value / float(service["total_cost"]) * 100.0)
            if float(service["total_cost"])
            else 0.0
        )

        details = service.get(meta["details_field"], {}) or {}
        detail_rows = []
        for component, value in details.items():
            value_float = float(value)
            if value_float == 0:
                continue
            detail_rows.append(
                {
                    "component": component,
                    "value": value_float,
                    "share": (value_float / selected_value * 100.0) if selected_value else 0.0,
                }
            )
        detail_rows.sort(key=lambda row: row["value"], reverse=True)

        peer_rows = []
        for peer in class_row["services"]:
            peer_value = float(peer.get(meta["field"], 0.0))
            peer_rows.append(
                {
                    "service_name": peer["service_name"],
                    "value": peer_value,
                    "is_current": peer["service_name"] == service_name,
                    "href": url_for(
                        "dashboard_cost_detail",
                        class_name=class_name,
                        service_name=peer["service_name"],
                        cost_key=cost_key,
                        source="cost" if source == "cost" else None,
                    ),
                }
            )
        peer_rows.sort(key=lambda row: row["value"], reverse=True)
        ranking = next(
            (idx + 1 for idx, row in enumerate(peer_rows) if row["is_current"]),
            0,
        )

        breadcrumbs = [
            {"name": "Главная", "href": url_for("dashboard_home")},
            {
                "name": class_name,
                "href": url_for("dashboard_class", class_name=class_name),
            },
            {
                "name": service_name,
                "href": url_for(
                    "dashboard_service",
                    class_name=class_name,
                    service_name=service_name,
                ),
            },
            {
                "name": meta["title"],
                "href": url_for(
                    "dashboard_cost_detail",
                    class_name=class_name,
                    service_name=service_name,
                    cost_key=cost_key,
                ),
            },
        ]

        return render_template(
            "dashboard_cost_detail.html",
            overview=overview,
            navigation=navigation,
            class_row=class_row,
            service=service,
            cost_key=cost_key,
            cost_meta=meta,
            selected_value=selected_value,
            selected_share=selected_share,
            detail_rows=detail_rows,
            peer_rows=peer_rows,
            ranking=ranking,
            current_level=4,
            current_class_name=class_name,
            current_service_name=service_name,
            current_cost_title=meta["title"],
            current_cost_key=cost_key,
            navigation_mode="cost" if source == "cost" else "class",
            cost_overview_href=url_for("dashboard_cost_overview", cost_key=cost_key),
            cost_class_href=url_for(
                "dashboard_cost_class",
                cost_key=cost_key,
                class_name=class_name,
            ),
            breadcrumbs=breadcrumbs,
        )
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        return render_dashboard_error(f"Ошибка построения страницы затрат: {exc}")


@app.route("/dashboard/legacy")
def dashboard_legacy():
    if not EXCEL_PATH.exists():
        return render_template(
            "dashboard.html",
            error=f"Файл не найден: {EXCEL_PATH}",
            dashboard=None,
        )

    try:
        dataset = load_calculation_services_dataset(EXCEL_PATH)
        dashboard_data = build_dashboard_data(dataset)
        return render_template("dashboard.html", error=None, dashboard=dashboard_data)
    except Exception as exc:
        return render_template(
            "dashboard.html",
            error=f"Ошибка построения дашборда: {exc}",
            dashboard=None,
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
