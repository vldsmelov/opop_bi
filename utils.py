from __future__ import annotations


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