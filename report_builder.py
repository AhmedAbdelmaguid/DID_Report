from __future__ import annotations

import json
from io import BytesIO
from typing import Any

import pandas as pd
from openpyxl.utils import get_column_letter

from utils import hangup_matches_normal_clearing, map_activity_from_campaign_series


def _count_unique_non_empty(series: pd.Series) -> int:
    return int(series.dropna().nunique())



def _join_unique_values(series: pd.Series) -> str:
    values = (
        series.astype("string")
        .fillna("")
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    return " | ".join(values)



def _prepare_working_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        raise ValueError("Nessun dato disponibile per generare il report.")

    working_df = detail_df.copy()
    working_df["attivita"] = working_df["attivita"].astype("string").fillna("").str.strip()
    working_df["did"] = working_df["did"].astype("string").fillna("").str.strip()
    working_df["campagna"] = working_df["campagna"].astype("string").fillna("").str.strip()
    working_df["cli"] = working_df["cli"].astype("string").fillna("").str.strip()
    working_df["talk_time"] = pd.to_numeric(working_df["talk_time"], errors="coerce").fillna(0)

    working_df = working_df[working_df["did"].ne("")].copy()
    if working_df.empty:
        raise ValueError("Nessun record valido: tutti i DID risultano vuoti.")

    working_df["cli"] = working_df["cli"].replace("", pd.NA)
    working_df["cli_risposta"] = working_df["cli"].where(working_df["is_risposta"], pd.NA)
    return working_df



def _apply_common_metrics(report_df: pd.DataFrame) -> pd.DataFrame:
    report_df["totale_chiamate"] = report_df["totale_chiamate"].astype(int)
    report_df["totale_risposte"] = report_df["totale_risposte"].astype(int)
    report_df["cli_unici"] = report_df["cli_unici"].astype(int)
    report_df["cli_unici_risposta"] = report_df["cli_unici_risposta"].astype(int)

    if "did_unici" in report_df.columns:
        report_df["did_unici"] = report_df["did_unici"].astype(int)
    if "campagne_uniche" in report_df.columns:
        report_df["campagne_uniche"] = report_df["campagne_uniche"].astype(int)
    if "attivita_uniche" in report_df.columns:
        report_df["attivita_uniche"] = report_df["attivita_uniche"].astype(int)

    report_df["talk_time_totale_secondi"] = report_df["talk_time_totale_secondi"].round(2)
    report_df["tasso_risposta"] = (
        report_df["totale_risposte"] / report_df["totale_chiamate"]
    ).fillna(0).round(4)
    report_df["tasso_risposta_cli_unici"] = (
        report_df["cli_unici_risposta"]
        / report_df["cli_unici"].replace(0, pd.NA)
    ).fillna(0).round(4)
    report_df["talk_time_totale_ore"] = (
        report_df["talk_time_totale_secondi"] / 3600
    ).round(2)
    return report_df



def build_detail_dataframe(
    original_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    talk_time_threshold_seconds: int,
    campaign_activity_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    detail_df = original_df.copy()
    detail_df["campagna"] = normalized_df["campagna"]
    detail_df["attivita"] = map_activity_from_campaign_series(
        normalized_df["campagna"],
        campaign_activity_mapping,
    )
    detail_df["did"] = normalized_df["did"]
    detail_df["cli"] = normalized_df["cli"]
    detail_df["hangup_cause"] = normalized_df["hangup_cause"]
    detail_df["talk_time"] = normalized_df["talk_time"]
    detail_df["hangup_matches_normal_clearing"] = hangup_matches_normal_clearing(
        detail_df["hangup_cause"]
    )
    detail_df["is_risposta"] = (
        detail_df["hangup_matches_normal_clearing"]
        & detail_df["talk_time"].fillna(0).gt(talk_time_threshold_seconds)
    )

    return detail_df



def build_activity_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    working_df = _prepare_working_df(detail_df)

    activity_summary_df = (
        working_df.groupby("attivita", dropna=False)
        .agg(
            totale_chiamate=("did", "size"),
            totale_risposte=("is_risposta", "sum"),
            cli_unici=("cli", _count_unique_non_empty),
            cli_unici_risposta=("cli_risposta", _count_unique_non_empty),
            did_unici=("did", _count_unique_non_empty),
            talk_time_totale_secondi=("talk_time", "sum"),
        )
        .reset_index()
    )

    activity_summary_df = _apply_common_metrics(activity_summary_df)
    activity_summary_df = activity_summary_df.sort_values(
        by=["totale_chiamate", "attivita"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)

    return activity_summary_df



def build_did_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    working_df = _prepare_working_df(detail_df)

    did_summary_df = (
        working_df.groupby("did", dropna=False)
        .agg(
            attivita_coinvolte=("attivita", _join_unique_values),
            campagne_coinvolte=("campagna", _join_unique_values),
            attivita_uniche=("attivita", _count_unique_non_empty),
            campagne_uniche=("campagna", _count_unique_non_empty),
            totale_chiamate=("did", "size"),
            totale_risposte=("is_risposta", "sum"),
            cli_unici=("cli", _count_unique_non_empty),
            cli_unici_risposta=("cli_risposta", _count_unique_non_empty),
            talk_time_totale_secondi=("talk_time", "sum"),
        )
        .reset_index()
    )

    did_summary_df = _apply_common_metrics(did_summary_df)
    did_summary_df = did_summary_df.sort_values(
        by=["totale_chiamate", "did"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)

    return did_summary_df



def build_report(detail_df: pd.DataFrame) -> pd.DataFrame:
    working_df = _prepare_working_df(detail_df)

    report_df = (
        working_df.groupby(["attivita", "campagna", "did"], dropna=False)
        .agg(
            totale_chiamate=("did", "size"),
            totale_risposte=("is_risposta", "sum"),
            cli_unici=("cli", _count_unique_non_empty),
            cli_unici_risposta=("cli_risposta", _count_unique_non_empty),
            talk_time_totale_secondi=("talk_time", "sum"),
        )
        .reset_index()
    )

    report_df = _apply_common_metrics(report_df)
    report_df = report_df.sort_values(
        by=["attivita", "campagna", "totale_chiamate", "did"],
        ascending=[True, True, False, True],
        kind="stable",
    ).reset_index(drop=True)

    return report_df



def _build_metadata_dataframe(metadata: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    for key, value in metadata.items():
        if isinstance(value, dict):
            display_value = json.dumps(value, ensure_ascii=False, indent=2)
        else:
            display_value = str(value)
        rows.append({"Campo": key, "Valore": display_value})

    return pd.DataFrame(rows)



def _autosize_worksheet(worksheet: Any) -> None:
    for column_cells in worksheet.columns:
        max_length = 0
        column_index = column_cells[0].column
        for cell in column_cells:
            cell_value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(cell_value))
        worksheet.column_dimensions[get_column_letter(column_index)].width = min(max_length + 2, 60)



def export_report_to_excel(
    activity_summary_df: pd.DataFrame,
    did_summary_df: pd.DataFrame,
    report_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    activity_assignments_df: pd.DataFrame,
    metadata: dict[str, Any],
) -> bytes:
    output_stream = BytesIO()

    try:
        with pd.ExcelWriter(output_stream, engine="openpyxl") as writer:
            activity_summary_df.to_excel(writer, index=False, sheet_name="Attivita")
            did_summary_df.to_excel(writer, index=False, sheet_name="Report DID")
            report_df.to_excel(writer, index=False, sheet_name="Report")
            detail_df.to_excel(writer, index=False, sheet_name="Dettaglio")
            activity_assignments_df.to_excel(writer, index=False, sheet_name="Associazioni")
            _build_metadata_dataframe(metadata).to_excel(
                writer,
                index=False,
                sheet_name="Metadata",
            )

            for worksheet in writer.sheets.values():
                _autosize_worksheet(worksheet)
    except Exception as exc:
        raise ValueError(f"Errore durante l'export Excel: {exc}") from exc

    output_stream.seek(0)
    return output_stream.getvalue()
