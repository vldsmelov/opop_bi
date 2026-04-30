from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from flask import Flask, abort, render_template, request, url_for
from werkzeug.exceptions import HTTPException

from dashboard_builder import (
    COST_TYPE_META,
    build_cost_scope_data,
    build_dashboard_data,
    build_navigation_data,
    get_cost_meta_or_404,
)
from data_loader import (
    load_calculation_services_by_class,
    load_calculation_services_dataset,
    load_workbook_sheets,
)

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
    description = getattr(error, "description", None) or "Страница не найдена"
    return render_dashboard_error(description, status_code=404)


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
