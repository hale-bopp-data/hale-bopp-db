# The Hale-Bopp Schema Observer
## Matrice delle Domande Agentiche (Schema Intelligence)

Per trasformare Hale-Bopp DB da un semplice "esecutore meccanico" a un **Agentic Schema Observer**, l'LLM deve poter rispondere a domande che normalmente richiederebbero ore di analisi manuale o complessi script SQL sui metadati (`information_schema` o `pg_catalog`). 

Ecco una categorizzazione delle domande d'oro, divise per caso d'uso ingegneristico reale.

---

### 1. 🧨 Impact Analysis & Refactoring (Analisi di Impatto)
*Evitare che una modifica spacchi il database in produzione.*

- "Se rinomino la colonna `status` in `order_status` nella tabella `ORDERS`, quali view, constraint o tabelle figlie si spaccano?"
- "Quali sono tutte le dipendenze a cascata se faccio il `DROP` della tabella `ARCHIVE_2020`?"
- "Esistono stored procedures o trigger che leggono la colonna `codice_fiscale` nella tabella `USERS`?"
- "Se sposto la chiave primaria di `INVOICES` da `INT` a `UUID`, quante chiavi esterne (Foreign Keys) in quante tabelle diverse devo aggiornare di conseguenza?"
- "Voglio fondere le tabelle `CUSTOMER_B2B` e `CUSTOMER_B2C` in un'unica tabella. Mi generi il piano di migrazione e mi dici quali regole di validazione entrerebbero in conflitto?"
- "Quante tabelle dipendono direttamente o indirettamente dalla tabella `CURRENCIES`?"

### 2. 🕵️ Security, Privacy & GDPR (Compliance)
*Garantire che i dati sensibili siano mappati e protetti.*

- "Quali tabelle del database potrebbero contenere PII (Personal Identifiable Information) o dati personali in base ai nomi delle colonne (es. email, phone, address, ssn)?"
- "Mostrami tutte le tabelle che contengono una colonna per le password in cui il tipo di dato non sembra criptato (es. manca il termine 'hash' o 'salt' oppure vedi campi `varchar(50)`)."
- "Ci sono colonne legate a dati sanitari o di fatturazione che non hanno alcun audit log trigger associato?"
- "Dammi un elenco di tutti i database roles/utenti che hanno permessi di cancellazione (`DELETE`) sulla tabella `PAYMENTS`."
- "Verifica se tutte le tabelle in cui compaiono carte di credito hanno abilitata la cifratura a livello di riga o colonna."

### 3. 🗺️ Onboarding & Architettura (Exploration)
*Per i nuovi developer che devono capire il dominio o i lead che non ricordano la mappa.*

- "Mi dai tutte le tabelle che hanno a che fare col concetto di 'Cliente' o 'Azienda'?"
- "Com'è modellato attualmente il processo di pagamento? Generami un mini diagramma ER testuale che mostra solo l'ecosistema che gira attorno a `PAYMENTS`."
- "Esiste già una tabella per gestire i consensi ai cookie? Se sì, come vi accedo a partire all'ID di un utente standard?"
- "Quali sono le 'Tabelle Isola' (Orphan Tables), ovvero quelle che non hanno nessuna Foreign Key in entrata o in uscita verso il resto del DB?"
- "Trova tutte le colonne che nel nome usano 'type' o 'status' ma che non hanno associato un dizionario (Enum) esplicito, né una foreign key a una tabella di anagrafica."
- "Dimmi qual è la tabella più centrale del sistema (quella connessa a più entità via FK)."

### 4. ⚡ Ottimizzazioni & Performance (Tech Debt)
*Rilevamento di anomalie progettuali e ottimizzazione indici.*

- "Elenca tutte le foreign key esistenti che non hanno un indice associato, e che quindi potrebbero causare table-scan lentissimi durante le JOIN."
- "Quali sono le 10 tabelle con il maggior numero di indici? C'è qualche indice duplicato o ridondante che possiamo eliminare?"
- "Trova le colonne che usano `VARCHAR(255)` ovunque come default pigro, ma che probabilmente dovrebbero essere `ENUM` o boolean."
- "Mostrami tutti gli indici che non seguono le nostre naming convention (dovrebbero iniziare con `idx_` o `uniq_`)."
- "Quali tipi di dato legacy o obsoleti (es. `serial` invece di `identity`, `timestamp` senza fuso orario, `text` in chiavi primarie) si nascondono nel database?"
- "Ci sono tabelle in cui viene usato UUID ma senza indici adatti, causando potenziale frammentazione?"

### 5. 🛠️ Sviluppo ed Estensione Mappa (Agentic Proposal)
*Chiedere all'agente di progettare l'evoluzione del database.*

- "Sto per sviluppare la feature del carrello della spesa B2B con sconti a scaglioni. Mi scrivi le tabelle `CART`, `CART_ITEMS` e `DISCOUNTS`, rispettando lo stile (snake_case) usato nel resto del DB?"
- "Genera uno script di seed temporaneo (Data Mock) di 100 righe per l'entità `USERS`, rispettando constraint e foreign keys esistenti."
- "Voglio implementare il *Soft Delete* (eliminazione logica) sulla tabella `PRODUCTS`. Crei tu l'alter table per la colonna `deleted_at` e aggiusti di conseguenza tutte le View che la leggono?"
- "Riscrivi i commenti/descrizioni (DDL `COMMENT ON`) per le 20 tabelle principali affinché il team data engineering possa capirci qualcosa."

### 6. 🚨 Drift & QA (Governance)
*Sinergia con le abilità uniche "meccaniche" di Hale-Bopp.*

- "L'engine di Hale-bopp ha rilevato una colonna intrusa `test_migrazione` nella tabella LIVE `users`. Riesci a capire dalle view o dall'history quando o perché è stata creata?"
- "Controlla le differenze (Diff) tra il DB di Staging e Produzione. Ignora le tabelle `logs_` e riassumimi solo le modifiche semantiche sui vincoli."
- "Ho bisogno di fare un Rollback del ticket #402. Dimmi quali sono tutti i field SQL che sono stati aggiunti e fammi una stima del rischio."

---
### Perché queste domande sono impossibili in altri tool?

Un tool normale come `pgAdmin` o `DBeaver` richiede che tu scriva query complesse su `/information_schema` facendo JOIN tra tabelle `pg_class`, `pg_attribute` e `pg_constraint` solo per sapere dove sta una colonna.

Hale-Bopp DB, possedendo al suo interno la fonte di verità (Il **Data Dictionary JSON**) e il grafo relazionale generato da **Maetel**, passa tutto come base di partenza (contesto) all'LLM. L'utente parla, l'LLM sonda questa mappa istantaneamente e risponde in secondi. 
