from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from utils import DEFAULT_CONFIG_PATH, LOGICAL_FIELDS, ensure_config_dir

DEFAULT_TALK_TIME_THRESHOLD_SECONDS = 40


def get_empty_config() -> dict[str, Any]:
    return {
        "column_mapping": {field_name: "" for field_name in LOGICAL_FIELDS},
        "talk_time_threshold_seconds": DEFAULT_TALK_TIME_THRESHOLD_SECONDS,
        "campaign_activity_overrides": {},
    }


def _sanitize_column_mapping(raw_mapping: Any) -> dict[str, str]:
    sanitized_mapping = {field_name: "" for field_name in LOGICAL_FIELDS}

    if raw_mapping is None:
        return sanitized_mapping

    if not isinstance(raw_mapping, dict):
        raise ValueError("La sezione 'column_mapping' della configurazione non e' valida.")

    for field_name in sanitized_mapping:
        value = raw_mapping.get(field_name)
        sanitized_mapping[field_name] = str(value).strip() if value else ""

    return sanitized_mapping


def sanitize_talk_time_threshold(value: Any) -> int:
    try:
        parsed_value = int(float(value))
    except (TypeError, ValueError):
        return DEFAULT_TALK_TIME_THRESHOLD_SECONDS

    return max(parsed_value, 0)


def _sanitize_campaign_activity_overrides(raw_overrides: Any) -> dict[str, str]:
    if raw_overrides is None:
        return {}

    if not isinstance(raw_overrides, dict):
        raise ValueError("La sezione 'campaign_activity_overrides' della configurazione non e' valida.")

    sanitized_overrides: dict[str, str] = {}
    for campaign_name, activity_name in raw_overrides.items():
        normalized_campaign_name = str(campaign_name).strip()
        normalized_activity_name = str(activity_name).strip() if activity_name else ""
        if normalized_campaign_name:
            sanitized_overrides[normalized_campaign_name] = normalized_activity_name

    return sanitized_overrides


def _sanitize_config(raw_config: Any) -> dict[str, Any]:
    if raw_config is None:
        return get_empty_config()

    if not isinstance(raw_config, dict):
        raise ValueError("La configurazione JSON non ha il formato atteso.")

    sanitized_config = get_empty_config()
    sanitized_config["column_mapping"] = _sanitize_column_mapping(raw_config.get("column_mapping"))
    sanitized_config["talk_time_threshold_seconds"] = sanitize_talk_time_threshold(
        raw_config.get("talk_time_threshold_seconds", DEFAULT_TALK_TIME_THRESHOLD_SECONDS)
    )
    sanitized_config["campaign_activity_overrides"] = _sanitize_campaign_activity_overrides(
        raw_config.get("campaign_activity_overrides")
    )
    return sanitized_config


def load_config(source: str | Path | bytes | bytearray | None = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    if source is None:
        return get_empty_config()

    try:
        if isinstance(source, (bytes, bytearray)):
            raw_config = json.loads(bytes(source).decode("utf-8-sig"))
            return _sanitize_config(raw_config)

        config_path = Path(source)
        if not config_path.exists():
            return get_empty_config()

        raw_config = json.loads(config_path.read_text(encoding="utf-8"))
        return _sanitize_config(raw_config)
    except json.JSONDecodeError as exc:
        raise ValueError("Il file di configurazione JSON e' corrotto o non leggibile.") from exc
    except OSError as exc:
        raise OSError(f"Errore durante la lettura della configurazione: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ValueError("La configurazione caricata non e' codificata in UTF-8.") from exc


def config_to_json(config: dict[str, Any]) -> str:
    return json.dumps(_sanitize_config(config), ensure_ascii=False, indent=2)


def save_config(
    config: dict[str, Any],
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> Path:
    ensure_config_dir()
    destination = Path(config_path)
    serialized_config = config_to_json(config)

    try:
        destination.write_text(serialized_config, encoding="utf-8")
    except OSError as exc:
        raise OSError(f"Errore durante il salvataggio della configurazione: {exc}") from exc

    return destination


def update_runtime_config(
    config: dict[str, Any] | None,
    column_mapping: dict[str, str | None],
    talk_time_threshold_seconds: int,
    campaign_activity_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    sanitized_config = _sanitize_config(config or get_empty_config())
    sanitized_config["column_mapping"] = {
        field_name: str(column_mapping.get(field_name) or "").strip()
        for field_name in LOGICAL_FIELDS
    }
    sanitized_config["talk_time_threshold_seconds"] = sanitize_talk_time_threshold(
        talk_time_threshold_seconds
    )
    sanitized_config["campaign_activity_overrides"] = _sanitize_campaign_activity_overrides(
        campaign_activity_overrides or sanitized_config.get("campaign_activity_overrides")
    )
    return sanitized_config
