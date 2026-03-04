# DB-HALE-BOPP: Release Management & Flyway Replacement

## Il Contesto Storico (Perché Flyway ha fallito)
Come documentato nella Wiki di EasyWayDataPortal (`why-not-flyway.md`), l'uso di engine di migrazione tradizionali come **Flyway** (o Liquibase) su database complessi come SQL Server si è rivelato un anti-pattern.
I motivi principali dell'abbandono di Flyway sono stati:
1. **Batch Separator Hell**: L'incapacità di Flyway di gestire nativamente e senza errori i separatori `GO` tra funzioni e Stored Procedures.
2. **Migration Version Chaos**: Il proliferare di dozzine di file `V1__...`, `V2__...` che corrompono lo schema se applicati parzialmente.
3. **Mancanza di Rollback Reale**: Difficoltà estrema nel tornare alla versione precedente in caso di fault.

EasyWay era passata a un approccio più crudo ma funzionale: **Git + SQL Diretto** (eseguito con `sqlcmd`).
Tuttavia, il SQL Diretto manuale perde la centralizzazione dell'Audit e l'automatismo delle CI/CD.

## La Soluzione: DB-HALE-BOPP come "Smart Executor"

**DB-HALE-BOPP sostituisce interamente la necessità di Flyway**, unendo la semplicità dell'approccio "SQL Diretto" con la robustezza di una piattaforma governata guidata dagli Agenti.

### 1. Dichiarativo vs Imperativo (Addio ai file V1, V2)
Con DB-HALE-BOPP, gli sviluppatori non scrivono file sequenziali di alterazione (`V2__add_column.sql`).
I Dev scrivono semplicemente lo **Stato Desiderato** (il file SQL completo o l'albero delle API).

Quando viene richiamata l'API `/schema/deploy`:
1. Il **Diff Engine** di DB-HALE-BOPP analizza lo stato *reale* del database nel momento esatto del rilascio.
2. Genera semanticamente la `ALTER TABLE` a runtime (usando l'AST interno).
3. La esegue in modalità transazionale.

### 2. Risoluzione dei Separatori di Batch (`GO`)
Poiché DB-HALE-BOPP usa librerie Python avanzate per parsare il SQL (come `sqlglot` e `SQLAlchemy`), spacchetta intelligentemente i comandi prima di inviarli al RDBMS. Non subisce la rigidità dei tool legacy scritti in Java come Flyway. Ogni ostruzione legata a `GO` o dipendenze tra SP viene calcolata dinamicamente dal grafo delle dipendenze del *Metadata Registry*.

### 3. Auditing Nativo e ARGOS M1
Mentre Flyway usava la banale tabella `flyway_schema_history`, DB-HALE-BOPP attiva una registrazione profonda nel suo *Metadata Registry*, triggerando gli eventi **ARGOS M1 (Fast-Ops)**.
* Se il deploy fallisce, l'Impact Analysis (Kortex Agent) allega subito nel log il probabile motivo logico e il DB torna allo stato precedente (Rollback Transazionale).

## Conclusione
DB-HALE-BOPP è l'evoluzione del "Git + SQL Diretto". Preserva la libertà del DBA di scrivere ed eseguire SQL nativo, ma lo intrappola in una rete di sicurezza orchestrata in tempo reale. Nessun file `V1__` da rinominare, nessun problema di compatibilità tra versioni, solo sincronizzazione tra *Target* e *Source of Truth*.
