# DID Report

App Streamlit locale per analizzare file Excel di chiamate telefoniche e generare report aggregati per attivita, campagna, DID e CLI unici.

## Funzionalita principali

- Upload di file Excel `.xlsx` e `.xls`
- Selezione del foglio da analizzare
- Mapping guidato delle colonne
- Regola risposta basata su `HangupCause` e soglia `talk_time`
- Associazione personalizzabile tra campagne e attivita
- Report per attivita, DID totale e dettaglio campagna + DID
- Export Excel con piu fogli

## Avvio locale

```bash
pip install -r requirements.txt
streamlit run app.py
```
