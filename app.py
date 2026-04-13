from __future__ import annotations

import hashlib
from datetime import datetime

import pandas as pd
import streamlit as st

from config_manager import (
    DEFAULT_TALK_TIME_THRESHOLD_SECONDS,
    get_empty_config,
    load_config,
    save_config,
    sanitize_talk_time_threshold,
    update_runtime_config,
)
from report_builder import (
    build_activity_summary,
    build_detail_dataframe,
    build_did_summary,
    build_report,
    export_report_to_excel,
)
from utils import (
    CAMPAIGN_ACTIVITY_SUFFIX_MAP,
    DEFAULT_CONFIG_PATH,
    LOGICAL_FIELDS,
    REQUIRED_FIELDS,
    SELECTION_PLACEHOLDER,
    build_activity_assignment_dataframe,
    build_campaign_activity_mapping,
    build_output_filename,
    compute_response_rule_statistics,
    get_sheet_names,
    load_excel_file,
    load_sheet_data,
    normalize_columns,
    sanitize_activity_assignment_dataframe,
    validate_activity_assignments,
    validate_column_mapping,
    validate_normalized_dataframe,
)

st.set_page_config(
    page_title="Analisi Chiamate Excel",
    layout="wide",
)

FIELD_LABELS = {
    "campagna": "Colonna Campagna",
    "did": "Colonna DID (numero aziendale chiamante)",
    "cli": "Colonna CLI (numero destinatario)",
    "hangup_cause": "Colonna HangupCause",
    "talk_time": "Colonna Talk Time in secondi",
}

FIELD_HELP = {
    "campagna": "Seleziona la colonna che identifica la campagna.",
    "did": "Seleziona la colonna con il numero aziendale da cui parte la chiamata.",
    "cli": "Seleziona la colonna con il numero chiamato o destinatario.",
    "hangup_cause": "Seleziona la colonna che contiene il valore HangupCause.",
    "talk_time": "Seleziona la colonna con il talk time espresso in secondi.",
}


def initialize_session_state() -> None:
    defaults = {
        "generated_activity_summary": None,
        "generated_did_summary": None,
        "generated_report": None,
        "generated_detail": None,
        "generated_activity_assignments": None,
        "generated_export_bytes": None,
        "generated_export_name": None,
        "generated_threshold_seconds": None,
        "activity_assignment_seed": None,
        "activity_assignment_signature": None,
        "activity_assignment_values": {},
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value



def reset_generated_outputs() -> None:
    st.session_state["generated_activity_summary"] = None
    st.session_state["generated_did_summary"] = None
    st.session_state["generated_report"] = None
    st.session_state["generated_detail"] = None
    st.session_state["generated_activity_assignments"] = None
    st.session_state["generated_export_bytes"] = None
    st.session_state["generated_export_name"] = None
    st.session_state["generated_threshold_seconds"] = None



def reset_activity_editor_state() -> None:
    st.session_state["activity_assignment_seed"] = None
    st.session_state["activity_assignment_signature"] = None
    st.session_state["activity_assignment_values"] = {}
    for session_key in list(st.session_state.keys()):
        if session_key.startswith("activity_input_"):
            del st.session_state[session_key]



def load_local_defaults() -> tuple[dict, str | None]:
    try:
        config = load_config(DEFAULT_CONFIG_PATH)
        return config, None
    except (ValueError, OSError) as exc:
        return get_empty_config(), str(exc)



def initialize_mapping_state(columns: list[str], default_mapping: dict[str, str]) -> None:
    for field_name in LOGICAL_FIELDS:
        state_key = f"mapping_{field_name}"
        current_value = st.session_state.get(state_key)
        default_value = default_mapping.get(field_name)
        valid_default = default_value if default_value in columns else SELECTION_PLACEHOLDER

        if current_value in columns or current_value == SELECTION_PLACEHOLDER:
            continue

        st.session_state[state_key] = valid_default



def build_mapping_ui(columns: list[str], default_mapping: dict[str, str]) -> dict[str, str | None]:
    initialize_mapping_state(columns, default_mapping)
    selections: dict[str, str | None] = {}

    for field_name in LOGICAL_FIELDS:
        state_key = f"mapping_{field_name}"
        current_value = st.session_state.get(state_key, SELECTION_PLACEHOLDER)
        other_selected_columns = {
            st.session_state.get(f"mapping_{other_field}")
            for other_field in LOGICAL_FIELDS
            if other_field != field_name
            and st.session_state.get(f"mapping_{other_field}") not in (None, "", SELECTION_PLACEHOLDER)
        }

        available_columns = [
            column_name
            for column_name in columns
            if column_name not in other_selected_columns or column_name == current_value
        ]
        options = [SELECTION_PLACEHOLDER] + available_columns

        if current_value not in options:
            default_value = default_mapping.get(field_name)
            st.session_state[state_key] = (
                default_value if default_value in options else SELECTION_PLACEHOLDER
            )

        st.selectbox(
            FIELD_LABELS[field_name],
            options=options,
            key=state_key,
            help=FIELD_HELP[field_name],
        )

        selected_value = st.session_state.get(state_key)
        selections[field_name] = (
            None if selected_value in (None, "", SELECTION_PLACEHOLDER) else selected_value
        )

    return selections



def build_display_report(report_df: pd.DataFrame) -> pd.DataFrame:
    display_df = report_df.copy()
    for rate_column in ["tasso_risposta", "tasso_risposta_cli_unici"]:
        if rate_column in display_df.columns:
            display_df[rate_column] = (
                display_df[rate_column] * 100
            ).round(2).map(lambda value: f"{value:.2f}%")
    return display_df



def build_activity_mapping_reference() -> pd.DataFrame:
    rows = [
        {"suffisso campagna": f"*_{activity_key}", "attivita automatica": activity_name}
        for activity_key, activity_name in CAMPAIGN_ACTIVITY_SUFFIX_MAP.items()
    ]
    return pd.DataFrame(rows)



def build_activity_assignment_signature(campaign_series: pd.Series) -> str:
    unique_campaigns = (
        campaign_series.astype("string").fillna("").str.strip().drop_duplicates().sort_values().tolist()
    )
    joined_campaigns = "||".join(unique_campaigns)
    return hashlib.md5(joined_campaigns.encode("utf-8")).hexdigest()



def get_activity_input_key(signature: str, campaign_name: str) -> str:
    campaign_hash = hashlib.md5(str(campaign_name).encode("utf-8")).hexdigest()[:12]
    return f"activity_input_{signature}_{campaign_hash}"



def ensure_activity_assignment_seed(
    campaign_series: pd.Series,
    saved_overrides: dict[str, str],
) -> tuple[pd.DataFrame, str]:
    signature = build_activity_assignment_signature(campaign_series)
    seed_df = build_activity_assignment_dataframe(campaign_series, saved_overrides)
    seed_mapping = {
        row["campagna"]: row["attivita_finale"]
        for _, row in seed_df.iterrows()
    }

    if st.session_state.get("activity_assignment_signature") != signature:
        st.session_state["activity_assignment_signature"] = signature
        st.session_state["activity_assignment_seed"] = seed_df
        st.session_state["activity_assignment_values"] = dict(seed_mapping)
        for session_key in list(st.session_state.keys()):
            if session_key.startswith("activity_input_"):
                del st.session_state[session_key]
    else:
        current_values = dict(st.session_state.get("activity_assignment_values", {}))
        valid_campaigns = set(seed_mapping)
        current_values = {campaign: value for campaign, value in current_values.items() if campaign in valid_campaigns}
        for campaign_name, activity_name in seed_mapping.items():
            current_values.setdefault(campaign_name, activity_name)
        st.session_state["activity_assignment_seed"] = seed_df
        st.session_state["activity_assignment_values"] = current_values

    return st.session_state["activity_assignment_seed"].copy(), signature



def build_current_activity_assignments(seed_df: pd.DataFrame) -> pd.DataFrame:
    current_values = st.session_state.get("activity_assignment_values", {})
    current_df = seed_df.copy()
    current_df["attivita_finale"] = current_df["campagna"].map(
        lambda campaign_name: str(current_values.get(campaign_name, "") or "").strip()
    )
    return sanitize_activity_assignment_dataframe(current_df)



def set_activity_for_campaigns(
    campaigns: list[str],
    activity_name: str,
    signature: str,
) -> None:
    updated_values = dict(st.session_state.get("activity_assignment_values", {}))
    normalized_activity_name = str(activity_name or "").strip()
    for campaign_name in campaigns:
        updated_values[campaign_name] = normalized_activity_name
        st.session_state[get_activity_input_key(signature, campaign_name)] = normalized_activity_name
    st.session_state["activity_assignment_values"] = updated_values



def render_activity_assignment_editor(
    assignments_df: pd.DataFrame,
    signature: str,
) -> pd.DataFrame:
    filter_col, blank_col = st.columns([2, 1])
    filter_text = filter_col.text_input(
        "Filtra campagne",
        key="activity_filter_text",
        placeholder="Scrivi parte del nome campagna",
    )
    show_only_blank = blank_col.checkbox(
        "Mostra solo attivita vuote",
        key="activity_show_only_blank",
        value=False,
    )

    filtered_df = assignments_df.copy()
    if filter_text:
        filter_value = filter_text.strip().lower()
        filtered_df = filtered_df[
            filtered_df["campagna"].str.lower().str.contains(filter_value, na=False)
        ]
    if show_only_blank:
        filtered_df = filtered_df[filtered_df["attivita_finale"].eq("")]

    st.caption(
        f"Campagne visibili nell'editor: {len(filtered_df)} su {len(assignments_df)}."
    )

    header_cols = st.columns([1.3, 0.8, 1.4, 1.8])
    header_cols[0].markdown("**campagna**")
    header_cols[1].markdown("**chiave automatica**")
    header_cols[2].markdown("**attivita proposta**")
    header_cols[3].markdown("**attivita finale modificabile**")

    updated_values = dict(st.session_state.get("activity_assignment_values", {}))

    for _, row in filtered_df.iterrows():
        campaign_name = row["campagna"]
        input_key = get_activity_input_key(signature, campaign_name)
        if input_key not in st.session_state:
            st.session_state[input_key] = row["attivita_finale"]

        cols = st.columns([1.3, 0.8, 1.4, 1.8])
        cols[0].write(campaign_name or "-")
        cols[1].write(row["chiave_automatica"] or "-")
        cols[2].write(row["attivita_proposta"] or "-")
        edited_value = cols[3].text_input(
            f"attivita_{campaign_name}",
            key=input_key,
            label_visibility="collapsed",
            placeholder="Inserisci nome attivita",
        )
        updated_values[campaign_name] = edited_value.strip()

    st.session_state["activity_assignment_values"] = updated_values
    return build_current_activity_assignments(st.session_state["activity_assignment_seed"])


initialize_session_state()

st.title("Analizzatore locale di chiamate da Excel")
st.write(
    "Carica un file Excel, scegli il foglio corretto, mappa le colonne e genera un report "
    "aggregato per campagna e DID."
)
st.info(
    "Regola risposta: una chiamata e' considerata risposta solo se HangupCause contiene "
    "'NormalClearing' e il talk time e' maggiore della soglia impostata."
)
st.caption(
    "Nel riepilogo attivita puoi unire piu' campagne sotto lo stesso nome oppure tenerle separate."
)

local_config, local_config_warning = load_local_defaults()
if local_config_warning:
    st.warning(
        "La configurazione locale non e' stata caricata correttamente. "
        "Usero' i valori di default. "
        f"Dettaglio: {local_config_warning}"
    )
else:
    st.caption(
        "L'app riutilizza automaticamente ultima mappatura, soglia e associazioni campagna-attivita salvate in locale."
    )

uploaded_file = st.file_uploader(
    "1. Carica un file Excel",
    type=["xlsx", "xls"],
    help="Sono supportati file Excel .xlsx e .xls.",
)

if not uploaded_file:
    st.info("Carica un file Excel per iniziare il flusso guidato.")
    st.stop()

uploaded_file_signature = f"{uploaded_file.name}-{uploaded_file.size}"
if st.session_state.get("uploaded_file_signature") != uploaded_file_signature:
    st.session_state["uploaded_file_signature"] = uploaded_file_signature
    reset_generated_outputs()
    reset_activity_editor_state()

try:
    excel_file = load_excel_file(uploaded_file)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

sheet_names = get_sheet_names(excel_file)
if not sheet_names:
    st.error("Il file Excel non contiene fogli utilizzabili.")
    st.stop()

selected_sheet = st.selectbox(
    "2. Seleziona il foglio da analizzare",
    options=sheet_names,
    help="Se il file contiene piu' fogli, scegli qui quello corretto.",
)

try:
    raw_df = load_sheet_data(excel_file, selected_sheet)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

if raw_df.empty:
    st.error("Il foglio selezionato e' vuoto.")
    st.stop()

st.subheader("3. Anteprima dati")
preview_col, info_col = st.columns([2, 1])

with preview_col:
    st.dataframe(raw_df.head(10), use_container_width=True)

with info_col:
    st.metric("Totale record caricati", len(raw_df))
    st.write("**Colonne disponibili**")
    st.dataframe(pd.DataFrame({"colonne": raw_df.columns}), use_container_width=True, height=280)

st.subheader("4. Mapping colonne")
if len(raw_df.columns) < len(REQUIRED_FIELDS):
    st.error(
        "Il foglio contiene meno di cinque colonne: non e' possibile completare il mapping obbligatorio."
    )
    st.stop()

column_mapping = build_mapping_ui(list(raw_df.columns), local_config.get("column_mapping", {}))
mapping_errors = validate_column_mapping(column_mapping)

if mapping_errors:
    for error_message in mapping_errors:
        st.error(error_message)
    st.stop()

try:
    normalized_df = normalize_columns(raw_df, column_mapping)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

validation_result = validate_normalized_dataframe(normalized_df)
for warning_message in validation_result["warnings"]:
    st.warning(warning_message)

if validation_result["errors"]:
    for error_message in validation_result["errors"]:
        st.error(error_message)
    st.stop()

valid_report_rows = int(normalized_df["did"].ne("").sum())
st.success("Mapping valido. I dati sono pronti per il calcolo del report.")
st.caption(
    f"Record validi per il report aggregato: {valid_report_rows} su {len(normalized_df)}."
)

st.subheader("5. Regola risposta")
default_threshold_seconds = sanitize_talk_time_threshold(
    local_config.get(
        "talk_time_threshold_seconds",
        DEFAULT_TALK_TIME_THRESHOLD_SECONDS,
    )
)
talk_time_threshold_seconds = int(
    st.number_input(
        "Soglia minima talk time in secondi",
        min_value=0,
        step=1,
        value=default_threshold_seconds,
        help="Default 40. Una chiamata e' risposta solo se talk_time e' maggiore di questa soglia.",
    )
)

rule_statistics = compute_response_rule_statistics(normalized_df, talk_time_threshold_seconds)
rule_metrics = st.columns(4)
rule_metrics[0].metric("Record con NormalClearing", rule_statistics["normal_clearing_records"])
rule_metrics[1].metric("Contatti con NormalClearing", rule_statistics["normal_clearing_unique_cli"])
rule_metrics[2].metric("Risposte finali", rule_statistics["response_records"])
rule_metrics[3].metric("Contatti risposta", rule_statistics["response_unique_cli"])

st.caption(
    "Record con HangupCause contenente NormalClearing ma esclusi per talk_time non sufficiente: "
    f"{rule_statistics['normal_clearing_under_threshold_records']} "
    f"su {rule_statistics['normal_clearing_under_threshold_unique_cli']} contatti."
)

st.subheader("6. Associazioni attivita")
st.write(
    "Qui puoi cambiare davvero il raggruppamento: assegna lo stesso nome attivita a piu' campagne per unirle "
    "nello stesso totale, oppure usa nomi diversi per tenerle separate."
)

with st.expander("Vedi regole automatiche di partenza"):
    st.dataframe(build_activity_mapping_reference(), use_container_width=True, hide_index=True)
    st.caption(
        "Le campagne senza proposta automatica restano vuote, cosi puoi compilarle manualmente."
    )

saved_activity_overrides = local_config.get("campaign_activity_overrides", {})
activity_assignment_seed, activity_signature = ensure_activity_assignment_seed(
    normalized_df["campagna"],
    saved_activity_overrides,
)
current_activity_assignments = build_current_activity_assignments(activity_assignment_seed)
all_campaigns = current_activity_assignments["campagna"].tolist()
proposal_map = current_activity_assignments.set_index("campagna")["attivita_proposta"].to_dict()

bulk_col_1, bulk_col_2 = st.columns([1.4, 1.6])
selected_campaigns = bulk_col_1.multiselect(
    "Campagne da modificare insieme",
    options=all_campaigns,
    key="bulk_assignment_campaigns",
    help="Seleziona una o piu' campagne da assegnare alla stessa attivita.",
)
bulk_activity_name = bulk_col_2.text_input(
    "Nome attivita da assegnare alle campagne selezionate",
    key="bulk_assignment_value",
    placeholder="Es. turisparmi energy unico",
)

apply_col, proposal_col, clear_col = st.columns(3)
if apply_col.button("Applica nome alle selezionate", use_container_width=True):
    if not selected_campaigns:
        st.warning("Seleziona almeno una campagna.")
    elif not bulk_activity_name.strip():
        st.warning("Inserisci il nome attivita da assegnare.")
    else:
        set_activity_for_campaigns(selected_campaigns, bulk_activity_name, activity_signature)
        st.rerun()

if proposal_col.button("Ripristina proposta automatica", use_container_width=True):
    if not selected_campaigns:
        st.warning("Seleziona almeno una campagna.")
    else:
        for campaign_name in selected_campaigns:
            set_activity_for_campaigns([campaign_name], proposal_map.get(campaign_name, ""), activity_signature)
        st.rerun()

if clear_col.button("Svuota attivita selezionate", use_container_width=True):
    if not selected_campaigns:
        st.warning("Seleziona almeno una campagna.")
    else:
        set_activity_for_campaigns(selected_campaigns, "", activity_signature)
        st.rerun()

reset_col, info_col = st.columns([1, 2])
if reset_col.button("Ripristina tutte le proposte automatiche", use_container_width=True):
    st.session_state["activity_assignment_seed"] = build_activity_assignment_dataframe(
        normalized_df["campagna"],
        {},
    )
    st.session_state["activity_assignment_values"] = {
        row["campagna"]: row["attivita_finale"]
        for _, row in st.session_state["activity_assignment_seed"].iterrows()
    }
    for session_key in list(st.session_state.keys()):
        if session_key.startswith("activity_input_"):
            del st.session_state[session_key]
    st.rerun()

with info_col:
    st.caption(
        "Puoi anche modificare riga per riga qui sotto. Se due campagne hanno lo stesso nome in 'attivita finale', verranno sommate insieme nel report attivita."
    )

current_activity_assignments = render_activity_assignment_editor(
    current_activity_assignments,
    activity_signature,
)
activity_assignment_errors = validate_activity_assignments(current_activity_assignments)
campaign_activity_mapping = build_campaign_activity_mapping(current_activity_assignments)
blank_activity_count = int(current_activity_assignments["attivita_finale"].eq("").sum())
final_activity_count = int(
    current_activity_assignments.loc[
        current_activity_assignments["attivita_finale"].ne(""),
        "attivita_finale",
    ].nunique()
)

assignment_metrics = st.columns(3)
assignment_metrics[0].metric("Campagne rilevate", len(current_activity_assignments))
assignment_metrics[1].metric("Attivita finali distinte", final_activity_count)
assignment_metrics[2].metric("Campagne senza attivita", blank_activity_count)

if blank_activity_count:
    st.warning(
        "Ci sono campagne senza attivita finale. Compilale per poter generare il report."
    )

for error_message in activity_assignment_errors:
    st.error(error_message)

generate_report_clicked = st.button(
    "Genera report",
    type="primary",
    use_container_width=True,
    disabled=bool(activity_assignment_errors),
)

if generate_report_clicked:
    try:
        runtime_config = update_runtime_config(
            config=local_config,
            column_mapping=column_mapping,
            talk_time_threshold_seconds=talk_time_threshold_seconds,
            campaign_activity_overrides=campaign_activity_mapping,
        )
        try:
            save_config(runtime_config, DEFAULT_CONFIG_PATH)
        except OSError as exc:
            st.warning(
                "Il report verra' generato comunque, ma non sono riuscito a salvare le impostazioni locali. "
                f"Dettaglio: {exc}"
            )

        detail_df = build_detail_dataframe(
            original_df=raw_df,
            normalized_df=normalized_df,
            talk_time_threshold_seconds=talk_time_threshold_seconds,
            campaign_activity_mapping=campaign_activity_mapping,
        )
        activity_summary_df = build_activity_summary(detail_df)
        did_summary_df = build_did_summary(detail_df)
        report_df = build_report(detail_df)

        metadata = {
            "Data elaborazione": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "File caricato": uploaded_file.name,
            "Foglio selezionato": selected_sheet,
            "Totale record caricati": len(raw_df),
            "Totale record validi per il report": valid_report_rows,
            "Soglia talk_time secondi": talk_time_threshold_seconds,
            "Regola risposta": "HangupCause contiene 'NormalClearing' e talk_time > soglia",
            "KPI CLI unici": "cli_unici_risposta / cli_unici",
            "Associazioni automatiche attivita": CAMPAIGN_ACTIVITY_SUFFIX_MAP,
            "Associazioni finali campagna-attivita": campaign_activity_mapping,
            "Mapping colonne": runtime_config["column_mapping"],
        }

        export_bytes = export_report_to_excel(
            activity_summary_df,
            did_summary_df,
            report_df,
            detail_df,
            current_activity_assignments,
            metadata,
        )

        st.session_state["generated_activity_summary"] = activity_summary_df
        st.session_state["generated_did_summary"] = did_summary_df
        st.session_state["generated_report"] = report_df
        st.session_state["generated_detail"] = detail_df
        st.session_state["generated_activity_assignments"] = current_activity_assignments
        st.session_state["generated_export_bytes"] = export_bytes
        st.session_state["generated_export_name"] = build_output_filename(
            uploaded_file.name,
            selected_sheet,
        )
        st.session_state["generated_threshold_seconds"] = talk_time_threshold_seconds
        st.success("Report generato con successo.")
    except ValueError as exc:
        st.error(str(exc))
    except Exception as exc:
        st.error(f"Errore inatteso durante la generazione del report: {exc}")

generated_activity_summary = st.session_state.get("generated_activity_summary")
generated_did_summary = st.session_state.get("generated_did_summary")
generated_report = st.session_state.get("generated_report")
generated_detail = st.session_state.get("generated_detail")
generated_activity_assignments = st.session_state.get("generated_activity_assignments")
generated_export_bytes = st.session_state.get("generated_export_bytes")
generated_export_name = st.session_state.get("generated_export_name")
generated_threshold_seconds = st.session_state.get("generated_threshold_seconds")

if (
    generated_activity_summary is not None
    and generated_did_summary is not None
    and generated_report is not None
    and generated_detail is not None
    and generated_activity_assignments is not None
    and generated_export_bytes is not None
):
    st.subheader("7. Risultati")

    reportable_detail = generated_detail[generated_detail["did"].astype("string").str.strip().ne("")].copy()
    reportable_detail["cli"] = reportable_detail["cli"].astype("string").fillna("").str.strip()
    total_campaigns = int(reportable_detail["campagna"].astype("string").nunique())
    total_activities = int(reportable_detail["attivita"].astype("string").nunique())
    total_dids = int(reportable_detail["did"].astype("string").nunique())
    total_answers = int(reportable_detail["is_risposta"].sum())
    overall_answer_rate = round(
        total_answers / len(reportable_detail),
        4,
    ) if len(reportable_detail) else 0.0
    overall_unique_cli = int(reportable_detail["cli"].replace("", pd.NA).dropna().nunique())
    overall_unique_cli_response = int(
        reportable_detail.loc[reportable_detail["is_risposta"], "cli"]
        .replace("", pd.NA)
        .dropna()
        .nunique()
    )
    overall_unique_cli_response_rate = round(
        overall_unique_cli_response / overall_unique_cli,
        4,
    ) if overall_unique_cli else 0.0
    total_talk_hours = generated_report["talk_time_totale_ore"].sum().round(2)

    metrics = st.columns(8)
    metrics[0].metric("Totale record caricati", len(raw_df))
    metrics[1].metric("Totale attivita", total_activities)
    metrics[2].metric("Totale campagne", total_campaigns)
    metrics[3].metric("Totale DID", total_dids)
    metrics[4].metric("Totale risposte", total_answers)
    metrics[5].metric("Tasso risposta complessivo", f"{overall_answer_rate * 100:.2f}%")
    metrics[6].metric("Tasso risposta CLI unici", f"{overall_unique_cli_response_rate * 100:.2f}%")
    metrics[7].metric("Totale ore parlate", f"{total_talk_hours:.2f}")

    st.caption(
        "Soglia applicata al report corrente: "
        f"{generated_threshold_seconds} secondi. "
        "Risposta = HangupCause contiene NormalClearing e talk_time > soglia."
    )

    activities_tab, did_tab, report_tab, assignments_tab, detail_tab = st.tabs(
        [
            "Report Attivita",
            "Report DID Totale",
            "Report Campagna + DID",
            "Associazioni Attivita",
            "Dettaglio",
        ]
    )

    with activities_tab:
        st.write("**Riepilogo totale per attivita**")
        st.dataframe(build_display_report(generated_activity_summary), use_container_width=True)

    with did_tab:
        st.write("**Riepilogo consolidato per DID su tutte le campagne**")
        st.dataframe(build_display_report(generated_did_summary), use_container_width=True)

    with report_tab:
        st.write("**Tabella aggregata per attivita, campagna e DID**")
        st.dataframe(build_display_report(generated_report), use_container_width=True)

    with assignments_tab:
        st.write("**Associazioni finali campagna -> attivita**")
        st.dataframe(generated_activity_assignments, use_container_width=True, hide_index=True)

    with detail_tab:
        st.write("**Anteprima dettaglio normalizzato**")
        st.dataframe(generated_detail.head(200), use_container_width=True)

    st.download_button(
        "Scarica report Excel",
        data=generated_export_bytes,
        file_name=generated_export_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
