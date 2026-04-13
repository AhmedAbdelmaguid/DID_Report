from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "call_report_config.json"

SELECTION_PLACEHOLDER = "-- Seleziona --"
EMPTY_TEXT_LABEL = "(VUOTO / NON VALORIZZATO)"
EMPTY_ACTIVITY_LABEL = ""

REQUIRED_FIELDS = ("campagna", "did", "cli", "hangup_cause", "talk_time")
LOGICAL_FIELDS = REQUIRED_FIELDS
CAMPAIGN_ACTIVITY_SUFFIX_MAP = {
    "en_dg": "turisparmi energy (comparatore)",
    "tr_ib": "turisparmi energy inbound",
    "en_ib": "turisparmi energy inbound",
    "tc_dg": "turisparmi telco comparatore",
}


def ensure_config_dir() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)


def detect_excel_engine(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix == ".xls":
        return "xlrd"
    return "openpyxl"


def load_excel_file(uploaded_file: Any) -> pd.ExcelFile:
    if uploaded_file is None:
        raise ValueError("Seleziona un file Excel prima di procedere.")

    filename = getattr(uploaded_file, "name", "")
    suffix = Path(filename).suffix.lower()
    if suffix not in {".xlsx", ".xls"}:
        raise ValueError("Formato non supportato. Carica un file .xlsx oppure .xls.")

    file_bytes = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()
    if not file_bytes:
        raise ValueError("Il file selezionato e' vuoto.")

    try:
        return pd.ExcelFile(BytesIO(file_bytes), engine=detect_excel_engine(filename))
    except Exception as exc:
        raise ValueError(
            "Impossibile aprire il file Excel. Verifica che il file non sia corrotto e riprova."
        ) from exc


def get_sheet_names(excel_file: pd.ExcelFile) -> list[str]:
    return [str(sheet_name) for sheet_name in excel_file.sheet_names]


def _sanitize_headers(columns: pd.Index) -> list[str]:
    sanitized_columns: list[str] = []

    for index, column_name in enumerate(columns, start=1):
        current_name = str(column_name).strip()
        if not current_name or current_name.lower().startswith("unnamed:"):
            current_name = f"colonna_{index}"
        sanitized_columns.append(current_name)

    return sanitized_columns


def load_sheet_data(excel_file: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    try:
        dataframe = excel_file.parse(sheet_name=sheet_name)
    except Exception as exc:
        raise ValueError(
            f"Impossibile leggere il foglio '{sheet_name}'. Controlla il contenuto del file Excel."
        ) from exc

    if dataframe is None:
        raise ValueError(f"Il foglio '{sheet_name}' non contiene dati leggibili.")

    dataframe = dataframe.dropna(how="all").copy()
    dataframe.columns = _sanitize_headers(dataframe.columns)
    return dataframe


def normalize_text_series(series: pd.Series, empty_value: str = "") -> pd.Series:
    normalized = series.astype("string").fillna("").str.strip()
    if empty_value:
        normalized = normalized.replace("", empty_value)
    return normalized


def prepare_talk_time_column(series: pd.Series) -> pd.Series:
    numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
    numeric_series = numeric_series.clip(lower=0)
    return numeric_series.astype(float)


def normalize_columns(dataframe: pd.DataFrame, column_mapping: dict[str, str | None]) -> pd.DataFrame:
    missing_columns = [
        source_column
        for source_column in column_mapping.values()
        if source_column and source_column not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError(
            "Alcune colonne selezionate non sono presenti nel foglio corrente: "
            + ", ".join(sorted(missing_columns))
        )

    normalized_df = pd.DataFrame(index=dataframe.index)
    normalized_df["campagna"] = normalize_text_series(dataframe[column_mapping["campagna"]])
    normalized_df["did"] = normalize_text_series(dataframe[column_mapping["did"]])
    normalized_df["cli"] = normalize_text_series(dataframe[column_mapping["cli"]])
    normalized_df["hangup_cause"] = normalize_text_series(
        dataframe[column_mapping["hangup_cause"]],
        empty_value=EMPTY_TEXT_LABEL,
    )
    normalized_df["talk_time"] = prepare_talk_time_column(dataframe[column_mapping["talk_time"]])

    return normalized_df


def validate_column_mapping(column_mapping: dict[str, str | None]) -> list[str]:
    errors: list[str] = []

    missing_fields = [field for field in REQUIRED_FIELDS if not column_mapping.get(field)]
    if missing_fields:
        errors.append(
            "Completa il mapping dei campi obbligatori: " + ", ".join(sorted(missing_fields)) + "."
        )

    selected_columns = [column_name for column_name in column_mapping.values() if column_name]
    duplicated_columns = sorted(
        {column_name for column_name in selected_columns if selected_columns.count(column_name) > 1}
    )
    if duplicated_columns:
        errors.append(
            "Ogni campo logico deve usare una colonna diversa. Colonne duplicate: "
            + ", ".join(duplicated_columns)
            + "."
        )

    return errors


def validate_normalized_dataframe(normalized_df: pd.DataFrame) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    if normalized_df.empty:
        errors.append("Il foglio selezionato e' vuoto dopo la lettura dei dati.")
        return {"errors": errors, "warnings": warnings}

    missing_did_records = int(normalized_df["did"].eq("").sum())
    empty_hangup_records = int(normalized_df["hangup_cause"].eq(EMPTY_TEXT_LABEL).sum())
    zero_talk_time_records = int(normalized_df["talk_time"].fillna(0).eq(0).sum())

    if normalized_df["did"].eq("").all():
        errors.append("La colonna DID e' vuota o contiene solo valori nulli.")
    elif missing_did_records:
        warnings.append(
            f"{missing_did_records} record hanno DID vuoto e verranno esclusi dal report aggregato."
        )

    if normalized_df["hangup_cause"].eq(EMPTY_TEXT_LABEL).all():
        errors.append("La colonna HangupCause e' vuota o contiene solo valori nulli.")
    elif empty_hangup_records:
        warnings.append(
            f"{empty_hangup_records} record hanno HangupCause vuoto e non verranno considerati come risposta."
        )

    if zero_talk_time_records:
        warnings.append(
            f"{zero_talk_time_records} record hanno talk_time pari a 0 o non numerico e potrebbero non superare la soglia minima."
        )

    return {
        "errors": errors,
        "warnings": warnings,
        "missing_did_records": missing_did_records,
        "empty_hangup_records": empty_hangup_records,
        "zero_talk_time_records": zero_talk_time_records,
    }


def hangup_matches_normal_clearing(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.contains("NormalClearing", case=False, na=False)


def extract_campaign_activity_key(campaign_name: str) -> str:
    normalized_campaign = str(campaign_name or "").strip().lower()
    if not normalized_campaign:
        return ""

    campaign_parts = [part for part in normalized_campaign.split("_") if part]
    if len(campaign_parts) >= 2:
        return "_".join(campaign_parts[-2:])

    return normalized_campaign


def get_activity_suggestion_from_campaign_value(campaign_name: str) -> str:
    activity_key = extract_campaign_activity_key(campaign_name)
    return CAMPAIGN_ACTIVITY_SUFFIX_MAP.get(activity_key, EMPTY_ACTIVITY_LABEL)


def get_activity_suggestion_from_campaign_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").map(get_activity_suggestion_from_campaign_value)


def build_activity_assignment_dataframe(
    campaign_series: pd.Series,
    campaign_activity_overrides: dict[str, str] | None = None,
) -> pd.DataFrame:
    overrides = campaign_activity_overrides or {}
    unique_campaigns = (
        campaign_series.astype("string").fillna("").str.strip().drop_duplicates().sort_values().tolist()
    )

    rows: list[dict[str, str]] = []
    for campaign_name in unique_campaigns:
        suggested_activity = get_activity_suggestion_from_campaign_value(campaign_name)
        final_activity = str(overrides.get(campaign_name, suggested_activity) or "").strip()
        rows.append(
            {
                "campagna": campaign_name,
                "chiave_automatica": extract_campaign_activity_key(campaign_name),
                "attivita_proposta": suggested_activity,
                "attivita_finale": final_activity,
            }
        )

    return pd.DataFrame(rows)


def sanitize_activity_assignment_dataframe(assignments_df: pd.DataFrame) -> pd.DataFrame:
    sanitized_df = assignments_df.copy()
    expected_columns = ["campagna", "chiave_automatica", "attivita_proposta", "attivita_finale"]
    for column_name in expected_columns:
        if column_name not in sanitized_df.columns:
            sanitized_df[column_name] = ""
        sanitized_df[column_name] = sanitized_df[column_name].astype("string").fillna("").str.strip()

    sanitized_df = sanitized_df[expected_columns].drop_duplicates(subset=["campagna"], keep="last")
    sanitized_df = sanitized_df.sort_values(by=["campagna"], kind="stable").reset_index(drop=True)
    return sanitized_df


def validate_activity_assignments(assignments_df: pd.DataFrame) -> list[str]:
    sanitized_df = sanitize_activity_assignment_dataframe(assignments_df)
    errors: list[str] = []

    blank_campaigns = sanitized_df.loc[sanitized_df["attivita_finale"].eq(""), "campagna"].tolist()
    if blank_campaigns:
        errors.append(
            "Completa il nome attivita per tutte le campagne prima di generare il report. "
            "Campagne senza attivita: "
            + ", ".join(blank_campaigns[:10])
            + (" ..." if len(blank_campaigns) > 10 else "")
        )

    return errors


def build_campaign_activity_mapping(assignments_df: pd.DataFrame) -> dict[str, str]:
    sanitized_df = sanitize_activity_assignment_dataframe(assignments_df)
    return {
        row["campagna"]: row["attivita_finale"]
        for _, row in sanitized_df.iterrows()
        if row["campagna"]
    }


def map_activity_from_campaign_value(
    campaign_name: str,
    campaign_activity_mapping: dict[str, str] | None = None,
) -> str:
    normalized_campaign_name = str(campaign_name or "").strip()
    if campaign_activity_mapping and normalized_campaign_name in campaign_activity_mapping:
        return str(campaign_activity_mapping[normalized_campaign_name] or "").strip()
    return get_activity_suggestion_from_campaign_value(normalized_campaign_name)


def map_activity_from_campaign_series(
    series: pd.Series,
    campaign_activity_mapping: dict[str, str] | None = None,
) -> pd.Series:
    return series.astype("string").fillna("").map(
        lambda campaign_name: map_activity_from_campaign_value(campaign_name, campaign_activity_mapping)
    )


def compute_response_rule_statistics(
    normalized_df: pd.DataFrame,
    talk_time_threshold_seconds: int,
) -> dict[str, int]:
    if normalized_df.empty:
        return {
            "total_records": 0,
            "normal_clearing_records": 0,
            "normal_clearing_unique_cli": 0,
            "above_threshold_records": 0,
            "above_threshold_unique_cli": 0,
            "response_records": 0,
            "response_unique_cli": 0,
            "normal_clearing_under_threshold_records": 0,
            "normal_clearing_under_threshold_unique_cli": 0,
        }

    working_df = normalized_df.copy()
    working_df["cli"] = working_df["cli"].astype("string").fillna("").str.strip()
    working_df["cli"] = working_df["cli"].replace("", pd.NA)

    normal_clearing_mask = hangup_matches_normal_clearing(working_df["hangup_cause"])
    above_threshold_mask = working_df["talk_time"].fillna(0).gt(talk_time_threshold_seconds)
    response_mask = normal_clearing_mask & above_threshold_mask
    normal_clearing_under_threshold_mask = normal_clearing_mask & ~above_threshold_mask

    return {
        "total_records": int(len(working_df)),
        "normal_clearing_records": int(normal_clearing_mask.sum()),
        "normal_clearing_unique_cli": int(working_df.loc[normal_clearing_mask, "cli"].dropna().nunique()),
        "above_threshold_records": int(above_threshold_mask.sum()),
        "above_threshold_unique_cli": int(working_df.loc[above_threshold_mask, "cli"].dropna().nunique()),
        "response_records": int(response_mask.sum()),
        "response_unique_cli": int(working_df.loc[response_mask, "cli"].dropna().nunique()),
        "normal_clearing_under_threshold_records": int(normal_clearing_under_threshold_mask.sum()),
        "normal_clearing_under_threshold_unique_cli": int(
            working_df.loc[normal_clearing_under_threshold_mask, "cli"].dropna().nunique()
        ),
    }


def safe_filename_part(value: str) -> str:
    cleaned_value = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return cleaned_value.strip("_") or "output"


def build_output_filename(uploaded_filename: str, sheet_name: str) -> str:
    file_stem = safe_filename_part(Path(uploaded_filename).stem)
    safe_sheet_name = safe_filename_part(sheet_name)
    return f"report_chiamate_{file_stem}_{safe_sheet_name}.xlsx"
