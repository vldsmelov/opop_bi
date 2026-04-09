from pathlib import Path

import pandas as pd
from flask import Flask, render_template


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


def load_workbook_sheets(path: Path) -> list[dict]:
    workbook = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    sheets_data = []

    for sheet_name, df in workbook.items():
        prepared_df = df.fillna("")
        records = prepared_df.to_dict(orient="records")
        columns = list(prepared_df.columns)
        sheets_data.append(
            {
                "name": sheet_name,
                "columns": columns,
                "rows": records,
                "row_count": len(records),
            }
        )

    return sheets_data


def find_column(
    columns: pd.Index,
    top_level_contains: str,
    sub_level_contains: str | None = None,
) -> tuple:
    for col in columns:
        if not isinstance(col, tuple) or len(col) < 2:
            continue

        top_level = str(col[0]).strip()
        sub_level = str(col[1]).strip()

        if top_level_contains not in top_level:
            continue

        if sub_level_contains is not None and sub_level_contains not in sub_level:
            continue

        return col

    raise ValueError(
        "Не удалось найти колонку: "
        f"top='{top_level_contains}', sub='{sub_level_contains}'"
    )


def load_calculation_services_by_class(path: Path) -> list[dict]:
    df = pd.read_excel(
        path,
        sheet_name="Калькуляция",
        header=[2, 3],
        engine="openpyxl",
    )

    number_col = find_column(df.columns, "№ п/п")
    class_col = find_column(df.columns, "Импорт / Экспорт")
    service_col = find_column(df.columns, "Наименование показателей")
    direct_total_col = find_column(df.columns, "Прямые расходы", "ИТОГО")
    indirect_total_col = find_column(df.columns, "Косвенные расходы", "ИТОГО")

    ineff_rent_col = find_column(df.columns, "Неэффективность", "Расходы на аренду")
    ineff_prt_rent_col = find_column(
        df.columns, "Неэффективность", "Расходы на аренду ПРТ (козловой кран)"
    )
    ineff_rzd_col = find_column(df.columns, "Неэффективность", "Расходы на услуги РЖД")

    selected_columns = [
        number_col,
        class_col,
        service_col,
        direct_total_col,
        indirect_total_col,
        ineff_rent_col,
        ineff_prt_rent_col,
        ineff_rzd_col,
    ]
    data = df[selected_columns].copy()
    data = data[pd.to_numeric(data[number_col], errors="coerce").notna()]

    data[class_col] = data[class_col].fillna("Без класса").astype(str).str.strip()
    data[service_col] = data[service_col].fillna("").astype(str).str.strip()
    data = data[data[service_col] != ""]
    data = data[data[service_col].str.casefold() != "обоснование"]

    numeric_columns = [
        direct_total_col,
        indirect_total_col,
        ineff_rent_col,
        ineff_prt_rent_col,
        ineff_rzd_col,
    ]
    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors="coerce").fillna(0.0)

    class_tables = []

    for class_name, group in data.groupby(class_col, sort=False):
        services = []
        class_direct_total = 0.0
        class_indirect_total = 0.0
        class_inefficiency_total = 0.0

        for _, row in group.iterrows():
            direct = float(row[direct_total_col])
            indirect_total = float(row[indirect_total_col])
            inefficiency = (
                float(row[ineff_rent_col])
                + float(row[ineff_prt_rent_col])
                + float(row[ineff_rzd_col])
            )
            indirect = indirect_total - inefficiency
            indirect_sum = indirect + inefficiency
            total = direct + indirect_sum

            services.append(
                {
                    "service_name": row[service_col],
                    "direct_cost": direct,
                    "indirect_cost": indirect,
                    "inefficiency_cost": inefficiency,
                    "indirect_sum": indirect_sum,
                    "total_cost": total,
                }
            )

            class_direct_total += direct
            class_indirect_total += indirect
            class_inefficiency_total += inefficiency

        class_indirect_sum_total = class_indirect_total + class_inefficiency_total

        class_tables.append(
            {
                "class_name": class_name,
                "services": services,
                "direct_total": class_direct_total,
                "indirect_total": class_indirect_total,
                "inefficiency_total": class_inefficiency_total,
                "indirect_sum_total": class_indirect_sum_total,
                "grand_total": class_direct_total + class_indirect_sum_total,
            }
        )

    return class_tables


def build_dashboard_data(class_tables: list[dict]) -> dict:
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

    top_services = sorted(
        all_services,
        key=lambda service: service["total_cost"],
        reverse=True,
    )[:12]

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
        largest_class = max(classes, key=lambda class_item: class_item["grand_total"])
        largest_class_name = largest_class["name"]
        largest_class_share = largest_class["share"]

    largest_service_name = "-"
    largest_service_total = 0.0
    if top_services:
        largest_service = max(top_services, key=lambda service: service["total_cost"])
        largest_service_name = largest_service["service_name"]
        largest_service_total = largest_service["total_cost"]

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
            "cost_structure_labels": [
                "Прямые расходы",
                "Косвенные расходы",
                "Неэффективность",
            ],
            "cost_structure_values": [
                direct_total,
                indirect_total,
                inefficiency_total,
            ],
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
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/excel-debug")
def excel_debug():
    if not EXCEL_PATH.exists():
        return render_template(
            "excel_debug.html",
            error=f"Файл не найден: {EXCEL_PATH}",
            sheets=[],
        )

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


@app.route("/dashboard")
def dashboard():
    if not EXCEL_PATH.exists():
        return render_template(
            "dashboard.html",
            error=f"Файл не найден: {EXCEL_PATH}",
            dashboard=None,
        )

    try:
        class_tables = load_calculation_services_by_class(EXCEL_PATH)
        dashboard_data = build_dashboard_data(class_tables)
        return render_template(
            "dashboard.html",
            error=None,
            dashboard=dashboard_data,
        )
    except Exception as exc:
        return render_template(
            "dashboard.html",
            error=f"Ошибка построения дашборда: {exc}",
            dashboard=None,
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
