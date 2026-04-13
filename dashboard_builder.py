from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd
from flask import abort

from .data_loader import load_calculation_services_dataset


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