# EPIC: Hale-Bopp DB - Universal Schema Governance Engine

**Description:**
Elevare l'Hale-Bopp DB Core Engine da un progetto di validazione locale a un "Universal Schema Governance Engine" di classe Enterprise (open source model). L'obiettivo è fari convergere il meglio dell'approccio DevOps/GitOps (stile **dbt**), della navigazione visuale interattiva (stile **dbdiagram.io**) e dell'ingegneria dei modelli complessi (stile **Erwin**), orchestrando il tutto con Intelligenza Artificiale Agentica Agnostica in configurazione *Testudo* (Separazione rigorosa tra LLM Planner ed Execution Engine Deterministico).
Fungere da ponte per sviluppatori ed architect, garantendo distribuzioni fluide (Zero-SQL), governance zero-drift nativa su hub VCS (GitHub/ADO) e sicurezza estrema.

**Target Scope:** EasyWay Workspace integration + General OS Release.
**Status:** Scoping / New
**Area:** Hale-Bopp OS Suite

---

## 🏗️ PBI-1: Schema Studio (Split-Screen DBML Editor) ✅ DONE
**Description:** Implementare ed elevare la web console `console.html` dotandola di un motore in tempo reale per lo "Schema Studio". Incorporare text-editor Code-like a sinistra (Ace/Monaco) e diagramma visuale (Mermaid / graph node engine) a destra. Modifica del sorgente JSON comporta rigenerazione del Canvas in `800ms` debounce, senza alcun salvataggio intermedio su DB live (Mock in-memory via `/maetel/dictionary`).
**Acceptance Criteria:**
- Split-screen UI robusta implementata in `/console`.
- Debounce rendering su modifica sorgente implementato.
- Gestione gracefully degli errori di parse JSON dell'utente lato UI.

## 🤖 PBI-2: Agentic Observer & LLM Router Integration (Caronte/MCP) ✅ DONE
**Description:** Sostituire il modulo Mock endpoint dell'Agentic Observer con chiamate reali via REST a LLMs target. Il sistema deve accettare un JSON context injection. Rispettare il principio *Bring Your Own LLM*. Deve poter operare consumando Azure OpenAI o sistemi agnostici via OpenRouter/Ollama come in Valentino Engine.
**Acceptance Criteria:**
- Rotta `/agent/ask` configurata per inoltrare la request col payload del dizionario al layer LLM.
- Implementata configurazione BYOL (API Key e Modelli via variables).
- La UI della chat renderizza Markdown correttamente per il syntax highlighting della risposta.

## 🛡️ PBI-3: "Testudo Formation" Execution Guardrails ✅ DONE
**Description:** L'Agentic Observer è autorizzato a rispondere a Q&A e generare modifiche "logiche" dello schema sotto forma esatta di segmenti JSON/DBML per essere riversata nell'Editor. Vietato all'LLM l'esecuzione di Raw SQL o connessioni fisiche. L'Agent deve poter invocare l'Aggiunta Colonna sul file JSON e lasciare l'onere del Rollback/SQL Generation al modulo `diff & plan` rigorosamente scritto nel backend Python.
**Acceptance Criteria:**
- Endpoint `plan` esegue una simulazione e blocca/avvisa su drop irreparabili.
- Creazione di un workflow "Approval/Gate" per applicare il piano dell'LLM sul vero DB.

## 🎨 PBI-4: Canvas Node-Based Evoluto e Visual Diffing ✅ DONE
**Description:** Abbandonare il motore visuale statico `mermaid.js` in favore di uno stack grafico basato a nodi (es. React Flow/Vis.js). La mappatura del Drift deve integrarsi con il frontend visivo: i differenziali tra il live database e il codice di progetto devono essere esposti nativamente nel canvas (ad es. colorando tabelle intruse in "rosso").
**Acceptance Criteria:**
- Migrazione rendering a libreria Drag&Drop JS supportata.
- Rilevazione del drift (via rotte esistenti) mappa una palette RAG (Rosso, Ambra, Verde) sul canvas delle tabelle disallineate.

## 🤝 PBI-5: Git-Native Integrations (ADO & GitHub Workflows) ✅ DONE
**Description:** Il motore Hale-Bopp funziona come validatore Git. Creazione di template Action/Pipeline (Azure DevOps e GitHub) in cui un Merge Request scaturisce uno `schema test`. L'engine ispeziona il JSON nel branch in focus diffato contro la "Staging connection" dichiarata, restituendo un feedback semantico (cosa si varcherà post-merge).
**Acceptance Criteria:**
- PR comment via API implementato: "Questa PR fa droppare la Tabella X".
- Pipeline YML exportables (easyway standard).

## 🚀 PBI-6: Advanced Tool: Reverse Engineering One-Click ✅ DONE
**Description:** Sviluppo funzione di *One-Click Introspect*. Avendo stringa di connessione Legacy, l'engine cattura per intero la struttura DB, analizzando tabelle, FK, indici (escludendo system view) e genera un bootstrap del `db-data-dictionary.json` standard, proiettando il database Legacy nell'Universo Hale-Bopp in 15 secondi.
**Acceptance Criteria:**
- Export pulito ed enumerato.
- Validatore sintattico pass-through post reverse engineering.

## 🧪 PBI-7: Advanced Tool: Mock Data (Seeder) Generator ✅ DONE
**Description:** L'infrastruttura di Agentic Observer prenderà in input il JSON file Dictionary, le chiavi PK e i Constraint per generare istruzioni di Mock (Seed Data). Poter generare JSON lines / CSV da usare per riempire il Database Target e completare gli Unit Test di Backend evitando Foreign Keys mancanti.
**Acceptance Criteria:**
- Output massivo di test data mockati tramite LLM e algoritmi Faker guidati dalla struttura JSON.
