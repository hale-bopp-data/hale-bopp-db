# DB-HALE-BOPP: Core API Design

Il backend Python di DB-HALE-BOPP (il **Governance Engine**) esporrà delle REST API (FastAPI) progettate per essere lo snodo centrale tra i comandi manuali, l'Agentic DBA (Kortex) e le automazioni CI/CD.

Queste API permetteranno l'interazione bidirezionale con il Database e il Metadata Registry, preparando il terreno per strati di governance superiori (come ARGOS-HALE-BOPP).

## 1. Endpoints di Validazione (Pre-Deployment)

### `POST /schema/validate`
Riceve uno schema target (JSON o DDL raw) e lo valida contro il *Rule Engine* interno.
* **Input**: Un payload contenente `{"table": "utenti", "columns": [...]}` o `CREATE TABLE utenti (...)`.
* **Output**: `200 OK` se lo schema rispetta le convenzioni aziendali (es. naming, tipi corretti, campi audit presenti) oppure `400 Bad Request` con la lista esatta delle regole fallite.
* **Actor**: Pipeline CI/CD o Kortex Agent prima di proporre una PR.

### `POST /schema/diff`
Calcola la differenza strutturale (Diff) tra lo schema proposto e il Database Reale/Metadata Registry in questo istante.
* **Input**: Lo schema target.
* **Output**: Un oggetto JSON che descrive l'Abstract Syntax Tree (AST) del delta (es. `{"add_columns": ["email"], "drop_columns": ["vecchia_email"]}`). Genera anche opzionalmente l'SQL di esecuzione nel dialetto specifico (Postgres/Snowflake).
* **Actor**: Applicazione Client o Kortex Agent per analizzare l'impatto tecnico.

## 2. Endpoints di Execution (Deployment)

### `POST /schema/deploy`
Riceve l'approvazione finale (magari con firma ARGOS o umana) ed esegue il Rollforward o Rollback.
* **Input**: Il diff validato + l'Approval Token.
* **Action**:
  1. Esegue le DDL sul DB target.
  2. Aggiorna il *Metadata Registry* interno.
  3. Scrive l'hash crittografico sull'audit trail.
* **Output**: `200 OK` con il `deployment_id`.

## 3. Endpoints Analitici & Intelligence (Drift & AI)

### `POST /drift/check` (The Sentinel)
Attivabile tramite Webhook (cron Airflow o Trigger nativo del DB).
* **Action**:
  1. Il motore ispeziona il dictionary del database vivo (`information_schema` o `ACCOUNT_USAGE`).
  2. Lo confronta contro la "Source of Truth" salvata nel suo DB interno.
* **Output**: `{"drift_detected": true, "drift_events": [...]}`.
* **Impact**: Se `drift_detected`, si invoca l'Agentic DBA o si allerta la catena di Governance (ARGOS).

### `POST /impact/analyze` (Hook for Kortex)
Riceve un Diff strutturale (generato da `/schema/diff` o `/drift/check`) e lo trasforma in metadati di impatto architetturale.
* **Input**: Oggetto Diff (es. colonna cancellata dalla tabella "ordini").
* **Output**: Albero delle dipendenze (Lineage) che esploderanno a seguito della modifica (es. "Attenzione: cancellare questa colonna romperà 2 Dashboard PowerBI e 1 pipeline ETL-HALE-BOPP").
* **Actor**: Questo è l'endpoint che fornisce la "Benzina" (il contesto tecnico) all'Assistente AI per potergli far scrivere il riassunto dell'impatto in Italiano per il Business.
