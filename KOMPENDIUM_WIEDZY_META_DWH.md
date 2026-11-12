# Meta DWH (Trino + Apache Iceberg + dbt) – Kompendium Wiedzy i Q&A

Kompletny zapis merytoryczny z sesji projektowej: pytania inżynierskie, odpowiedzi architektoniczne, rozbicie kodu na czynniki pierwsze oraz wyjaśnienie nietrywialnych mechanizmów bazodanowych.

---

## SPIS TREŚCI
1. [Fundamenty: Trino, Apache Iceberg, Parquet i AWS S3](#1-fundamenty-trino-apache-iceberg-parquet-i-aws-s3)
2. [Problem Biznesowy i Pułapka Podwójnego Zliczania (Double Counting)](#2-problem-biznesowy-i-pułapka-podwójnego-zliczania)
3. [Architektura Modelu Gwiazdy (Kimball Star Schema)](#3-architektura-modelu-gwiazdy-kimball-star-schema)
4. [Rola Poszczególnych Plików w Projekcie](#4-rola-poszczególnych-plików-w-projekcie)
5. [Głęboka Analiza Kodu i Nietrywialnych Konstrukcji SQL](#5-głęboka-analiza-kodu-i-nietrywialnych-konstrukcji-sql)
   - [Dlaczego CROSS JOIN UNNEST i czym to się różni od zwykłego iloczynu kartezjańskiego](#a-dlaczego-cross-join-unnest)
   - [Dlaczego UNION ALL i po co wstawiamy rekord __UNKNOWN_USER__](#b-dlaczego-union-all-i-rekord-__unknown_user__)
   - [Dlaczego algorytm MD5 w Data Warehousingu (Surrogate Keys)](#c-dlaczego-algorytm-md5)
   - [Akumulacja Stanu w dim_table (coalesce, least, greatest)](#d-akumulacja-stanu-w-dim_table)
   - [Trzypoziomowa przestrzeń nazw Trino i parser identyfikatorów](#e-trzypoziomowa-przestrzeń-nazw-trino)
6. [Jak dbt Współpracuje z Icebergiem (.yml i Snapshoty)](#6-jak-dbt-współpracuje-z-icebergiem-yml-i-snapshoty)
7. [Zapytania Biznesowe (Analyses)](#7-zapytania-biznesowe-analyses)

---

## 1. Fundamenty: Trino, Apache Iceberg, Parquet i AWS S3

### Czym jest Trino?
* **Trino NIE JEST bazą danych.** Nie posiada własnego dysku na dane.
* Jest **rozproszonym silnikiem zapytań SQL (Distributed MPP Query Engine)**, stworzonym pierwotnie w Facebooku (jako Presto).
* Działa jako potężny kalkulator w pamięci RAM: jeden Koordynator dzieli zapytanie SQL na zadania i rozrzuca je na setki maszyn (Workerów).
* Potrafi łączyć dane leżące w różnych miejscach (S3, Postgres, Kafka) w jednym zapytaniu (*Query Federation*).

### Czym jest Apache Iceberg i dlaczego sam S3 to za mało?
* S3 to po prostu „worek na pliki” (Object Storage). Brak w nim blokad (*locks*), atomowości czy transakcyjności. Gdy zapis padnie w połowie, dane są uszkodzone.
* **Apache Iceberg to Open Table Format** (specyfikacja formatu + biblioteka wbudowana m.in. w Trino).
* Nadzoruje pliki kolumnowe **Parquet** na S3 za pomocą drzewa metadanych JSON/Avro.
* **Snapshoty i ACID:** Każdy zapis (`INSERT`, `MERGE`) tworzy nowy plik metadanych (snapshot) i atomowo przestawia wskaźnik. Czytelnicy zawsze widzą spójny stan tabeli. Zapewnia to transakcje ACID, brak narzutu pamięciowego i podróże w czasie (*Time Travel*).

### Dlaczego pliki to Parquet, a nie CSV?
* **Format kolumnowy:** Zapytanie `SELECT count(distinct user_name)` czyta z S3 tylko jedną kolumnę. Pozostałe 99 kolumn jest ignorowane.
* **Kompresja:** Jednorodne typy danych w kolumnach kompresują się o 80–90% lepiej niż wiersze tekstowe.

---

## 2. Problem Biznesowy i Pułapka Podwójnego Zliczania

### Co zbieramy?
Trino ma mechanizm `EventListener`, który po zakończeniu każdego zapytania zrzuca surowy rekord `QueryCompletedEvent` na S3 do tabeli `raw_trino_events`.

### Problem 1: Brak danych organizacyjnych w logach
W logu Trino widnieje tylko login systemowy: `user_name = 'jkowalski'`. Trino nie wie, czy Kowalski pracuje w Marketingu, czy w Ryzyku. Dane te musimy dołączyć z zewnętrznego źródła HR (`seed_user_teams.csv`).

### Problem 2: Zagnieżdżona lista tabel
W logu Trino odpytane tabele są zapisane jako tablica w pojedynczej komórce:
`tables = ['iceberg.sales.orders', 'iceberg.crm.customers']`.
Nie da się po tym łatwo i wydajnie filtrować relacyjnym SQL-em.

### Problem 3: Pułapka podwójnego zliczania zasobów (Double Counting)
Zapytanie Kowalskiego trwało 10 sekund CPU i odpytało 2 tabele (`orders` i `customers`).
* Jeśli przypiszemy po 10s do każdej tabeli, to sumując zużycie tabel otrzymamy **20 sekund CPU** – sfałszowany raport dla CFO!
* **Rozwiązanie architektoniczne:**
  1. **`fact_query_execution` (Grain: 1 wiersz = 1 zapytanie):** Zachowuje oryginalne 10s CPU. Służy do liczenia całkowitego kosztu klastra i zespołów.
  2. **`fact_table_usage` (Grain: 1 wiersz = 1 zapytanie $\times$ 1 tabela):** Dzieli koszt proporcjonalnie ($1/N$): przypisuje po 5s CPU do `orders` i 5s do `customers`.

---

## 3. Architektura Modelu Gwiazdy (Kimball Star Schema)

```
                    +------------------------------------+
                    |  raw_trino_events (Iceberg on S3)  |
                    +------------------------------------+
                                      |
                                      v
                    +------------------------------------+
                    |        stg_raw_trino_events        |
                    |      (Grain: 1 row = 1 query)      |
                    +------------------------------------+
                                      |
                     +----------------+----------------+
                     |                                 |
                     v                                 v
    +--------------------------------+   +---------------------------+
    |   stg_raw_trino_event_tables   |   |   seed_user_teams (HR)    |
    | (Grain: 1 row = 1 query x tbl) |   +---------------------------+
    +--------------------------------+                 |
          |                     |                      v
          |                     |             +-----------------+
          |                     +------------>|    dim_user     |
          v                                   +-----------------+
    +-----------+                                      |
    | dim_table |                                      |
    +-----------+                                      |
          |                     +----------------------+
          |                     |                      |
          v                     v                      v
+------------------+  +-------------------+  +----------------------+
|    dim_table     |  | fact_table_usage  |  | fact_query_execution |
+------------------+  +-------------------+  +----------------------+
```

---

## 4. Rola Poszczególnych Plików w Projekcie

| Plik | Typ | Rola w systemie |
| :--- | :--- | :--- |
| `dbt_project.yml` | Konfiguracja dbt | Globalny zarządca: wymusza format `ICEBERG`, strategię `incremental` dla core i `view` dla stagingu. |
| `profiles.yml` | Konfiguracja połączenia | Definiuje parametry połączenia dbt z klastrem Trino (`host`, `port`, `catalog`, `schema`). |
| `seeds/seed_user_teams.csv` | Dane słownikowe (HR) | Mapowanie użytkowników na zespoły (`team_name`), działy (`department`) i centra kosztowe. |
| `macros/surrogate_key.sql` | Makro dbt / Jinja | Generuje bezkolizyjne, deterministyczne klucze sztuczne MD5 kompatybilne z Trino SQL. |
| `models/staging/sources.yml` | Kontrakt Danych | Definiuje tabelę źródłową `raw_trino_events` i nakłada testy integralności. |
| `models/staging/stg_raw_trino_events.sql` | Widok Staging (1:1) | Filtruje zapytania `FINISHED`, rzutuje typy numeryczne i czasowe, wylicza datę i liczbę tabel $N$. |
| `models/staging/stg_raw_trino_event_tables.sql` | Widok Staging (1:N) | Rozbija tablicę tabel przez `CROSS JOIN UNNEST`, parsuje identyfikatory, oblicza wagę alokacji $1/N$. |
| `models/core/dim_user.sql` | Wymiar (Iceberg Table) | Słownik użytkowników: łączy HR z logami, obsługuje service accounts i dodaje rekord `__UNKNOWN_USER__`. |
| `models/core/dim_table.sql` | Wymiar (Iceberg Incremental) | Rejestr tabel: zlicza zapytania *lifetime*, aktualizuje daty pierwszego i ostatniego odpytania tabeli. |
| `models/core/fact_query_execution.sql` | Fakt (Iceberg Incremental) | Ziarno zapytania (brak duplikacji). Odpowiada na pytanie o łączne zużycie zasobów klastra i zespołów. |
| `models/core/fact_table_usage.sql` | Fakt (Iceberg Incremental) | Ziarno tabela-zapytanie z alokacją kosztu ($1/N$). Odpowiada na pytanie o popularność tabeli X. |
| `analyses/*.sql` | Gotowe zapytania analityczne | Przykładowe zapytania SQL odpowiadające wprost na pytania biznesowe z zadania rekrutacyjnego. |

---

## 5. Głęboka Analiza Kodu i Nietrywialnych Konstrukcji SQL

### A. Dlaczego `CROSS JOIN UNNEST`?
```sql
FROM base_events
CROSS JOIN UNNEST(base_events.tables) AS t(raw_table_identifier)
```
* **Problem:** W ANSI SQL nie można napisać `JOIN base_events.tables`, bo `tables` to kolumna wewnątrz wiersza, a nie zarejestrowana tabela w bazie.
* **Działanie UNNEST:** Zamienia tablicę `['orders', 'customers']` z bieżącego wiersza w wirtualną mini-tabelkę o 2 wierszach.
* **Dlaczego CROSS JOIN:** W algebrze relacyjnej jedynym sposobem na połączenie 1 wiersza z listą 2 wierszy bez wspólnego klucza (`ON a = b`) jest iloczyn kartezjański.
* **Czy grozi wybuchem pamięci?** **NIE.** To jest tzw. *Correlated Join*. Wykonuje się lokalnie w pamięci RAM dla każdego wiersza z osobna:
  $1\text{ wiersz} \times 2\text{ elementy tablicy} = 2\text{ wiersze na wyjściu}$. Sumaryczna liczba wierszy to po prostu suma elementów we wszystkich tablicach.

---

### B. Dlaczego `UNION ALL` i rekord `__UNKNOWN_USER__`?
```sql
final as (
    select ... from combined_users
    union all
    select
        '__UNKNOWN_USER__' as user_key,
        'unknown' as user_name,
        'Unassigned / Service Account' as team_name,
        'Unassigned Department' as department,
        'CC-UNASSIGNED' as cost_center,
        true as is_service_account,
        current_timestamp as dwh_updated_at
)
```
* **Różnica UNION vs UNION ALL:** `UNION` deduplikuje wiersze (ciężkie sortowanie w pamięci). `UNION ALL` bezmyślnie dokleja wiersz pod spodem – jest natychmiastowe.
* **Rola rekordu-strażnika (Sentinel Record):**
  W tabeli faktów mamy kod:
  ```sql
  coalesce(u.user_key, '__UNKNOWN_USER__') as user_key
  ```
  Jeśli zapytanie odpalił bot, konto techniczne (`airflow`) lub usunięty pracownik, którego nie ma w pliku HR, klucz obcy nie będzie `NULL-em`.
* **Czy ten rekord się potem usuwa?**
  **Absolutnie NIE!** Rekord zostaje w bazie na zawsze. Gdybyśmy go usunęli, analityk robiący w PowerBI `INNER JOIN fact_query_execution f JOIN dim_user u ON f.user_key = u.user_key` wyrzuciłby te zapytania do kosza, a rachunek z AWS przestałby się zgadzać. Dzięki temu rekordowi koszty te trafiają do czytelnej kategorii *„Unassigned / Service Account”*.

---

### C. Dlaczego algorytm MD5 w Data Warehousingu?
```sql
lower(to_hex(md5(to_utf8(
    concat_ws('||',
        coalesce(cast(field1 as varchar), '__dbt_null__'),
        coalesce(cast(field2 as varchar), '__dbt_null__')
    )
))))
```
* **MD5 to ten sam algorytm co w kryptografii**, ale używamy go do **hashingu danych**, a nie do zabezpieczania haseł.
* **Zalety:**
  1. **Determinizm (Idempotencja):** `MD5('jkowalski')` zawsze daje ten sam wynik. Brak problemów z rozjeżdżaniem się auto-incrementów przy ponownym ładowaniu dbt.
  2. **Stała szerokość (Fixed-width):** Dowolnie długi string zamienia w 32-znakowy ciąg heksadecymalny (szybkie złączenia w bazach kolumnowych).
  3. **Wydajność:** Jest wielokrotnie szybszy w obliczeniach na klastrze niż SHA-256.
* **Triki w makrze:**
  - `concat_ws('||', ...)` zapobiega kolizjom sklejenia (np. `a` + `bc` vs `ab` + `c`).
  - `to_utf8(...)` i `to_hex(...)` są konieczne, ponieważ Trino operuje na typie binarnym `VARBINARY`.
  - `coalesce(..., '__dbt_null__')` chroni przed zamianą całego hasha w `NULL` w przypadku pustych wartości.

---

### D. Akumulacja Stanu w `dim_table`
```sql
coalesce(ext.total_queries_lifetime, 0) + inc.query_count_batch as total_queries_lifetime,
coalesce(least(ext.first_queried_at, inc.first_queried_at), inc.first_queried_at) as first_queried_at,
coalesce(greatest(ext.last_queried_at, inc.last_queried_at), inc.last_queried_at) as last_queried_at
```
* **`ext` (existing):** Dane historyczne zapisane w tabeli do wczoraj.
* **`inc` (incoming):** Nowa paczka danych z dzisiaj.
* **Mechanika działania:**
  - `total_queries_lifetime`: Bierze wczorajszy licznik (np. 100) i dodaje dzisiejsze zapytania (np. 5) = 105. Jeśli tabela pojawiła się dziś po raz pierwszy (`ext` to NULL), `coalesce` wstawia 0: $0 + 5 = 5$.
  - `least(...)`: Wybiera datę wcześniejszą (zachowuje datę pierwszego pojawienia się tabeli w historii).
  - `greatest(...)`: Wybiera datę późniejszą (aktualizuje znacznik ostatniej aktywności).
* **Zysk wydajnościowy:** Pipeline jest w 100% inkrementalny – nie musi przeliczać historii od początku świata.

---

### E. Trzypoziomowa przestrzeń nazw Trino i odporny parser
W Trino hierarchia obiektów wygląda tak:
$$\text{KATALOG} \;\longrightarrow\; \text{SCHEMAT} \;\longrightarrow\; \text{TABELA}$$

Użytkownik w konsoli może odpytać:
1. `iceberg.sales.orders` (3 człony – podał pełną ścieżkę)
2. `sales.orders` (2 człony – ustawił domyślny katalog w sesji: `USE iceberg;`)
3. `orders` (1 człon – ustawił katalog i schemat: `USE iceberg.sales;`)

Nasz kod sprawdza `cardinality(split(raw_table_identifier, '.'))` przed pobraniem indeksu. Dzięki temu zapytanie jednoczłonowe nie rzuca błędu `Array subscript out of bounds`.

---

## 6. Jak dbt Współpracuje z Icebergiem (.yml i Snapshoty)

### Wymuszenie formatu Iceberg
W `dbt_project.yml` oraz w blokach `{{ config(...) }}` wpisujemy:
```yaml
properties:
  format: "'ICEBERG'"
  partitioning: "ARRAY['day(execution_date)']"
```
Podczas kompilacji dbt generuje instrukcję DDL dla Trino:
`CREATE TABLE ... WITH (format = 'ICEBERG', partitioning = ARRAY['day(execution_date)'])`.

### Kiedy i jak powstają snapshoty Iceberga?
* **Automatycznie przy każdym zapisie:** dbt wykonuje `MERGE INTO target USING staging...`.
* Trino zapisuje nowe pliki Parquet na S3.
* Trino generuje nowy plik `vX.metadata.json` z nowym snapshotem i atomowo przestawia wskaźnik.
* **Hidden Partitioning:** Dzięki funkcji wirtualnej `day(execution_date)` zapytania analityczne z filtrem daty automatycznie odcinają niepotrzebne pliki Parquet bez czytania ich z dysku S3 (*partition pruning*).

---

## 7. Zapytania Biznesowe (Analyses)

### Pytanie 1: Ilu unikalnych użytkowników odpytało tabelę X w danym czasie?
* **Źródło:** `fact_table_usage` (właściwe ziarno: tabela $\times$ zapytanie).
```sql
select
    full_table_name,
    count(distinct user_key) as distinct_users_count,
    min(start_time) as period_start,
    max(start_time) as period_end,
    count(query_id) as total_queries_count,
    round(sum(allocated_cpu_time_ms) / 1000.0, 2) as total_allocated_cpu_seconds,
    round(sum(allocated_bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 3) as total_allocated_gb_scanned
from {{ ref('fact_table_usage') }}
where full_table_name = 'iceberg.sales.orders'
  and execution_date between date '2026-11-01' and date '2026-11-07'
group by
    full_table_name;
```

### Pytanie 2: Ile zasobów zużył zespół Y w danym czasie?
* **Źródło:** `fact_query_execution` (ziarno: zapytanie – brak ryzyka duplikacji kosztów).
```sql
select
    team_name,
    department,
    count(query_id) as total_queries_executed,
    round(sum(cpu_time_ms) / (1000.0 * 60.0 * 60.0), 2) as total_cpu_hours,
    round(sum(bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 2) as total_gb_scanned,
    round(avg(duration_ms) / 1000.0, 2) as avg_query_duration_seconds,
    round(max(peak_memory_bytes) / (1024.0 * 1024.0 * 1024.0), 2) as max_peak_memory_gb
from {{ ref('fact_query_execution') }}
where team_name = 'Marketing Analytics'
  and execution_date between date '2026-11-01' and date '2026-11-30'
group by
    team_name,
    department;
```

---

## 8. Zaawansowane Metryki FinOps: Parallelism Factor

W pliku `analyses/query_efficiency_finops.sql` wprowadziliśmy wskaźnik efektywności wielordzeniowej klastra:
```text
parallelism_factor = cpu_time_ms / duration_ms
```
* **Co to mierzy:** Średnią liczbę rdzeni procesora, które aktywnie pracowały nad zapytaniem w danej jednostce czasu.
* **Dlaczego to kluczowe dla platformy:**
  - `parallelism_factor >= 8.0`: Zapytanie doskonale zrównoleglone na workerach Trino MPP.
  - `parallelism_factor ≈ 1.0` przy dużym zużyciu CPU: **Wąskie gardło sekwencyjne**. Analityk napisał zapytanie, które zablokowało jeden rdzeń na jednej maszynie (np. brak partycjonowania, nieoptymalny join), blokując kolejkę klastra.

---

## 9. Środowisko DevOps: CI/CD i Docker Compose

### A. GitHub Actions CI Pipeline (`.github/workflows/ci.yml`)
* **Dlaczego to istnieje:** Zapobiega wrzucaniu zepsutego kodu na produkcję.
* **Jak działa:** Przy każdym `git push` na gałąź `main`:
  1. GitHub uruchamia kontener Ubuntu.
  2. Instaluje środowisko `Python 3.12` oraz `dbt-core` i `dbt-trino`.
  3. Wykonuje komendę `dbt parse --profiles-dir .`, weryfikując składnię wszystkich modeli, makr i 46 testów relacyjnych YAML.
  4. Sukces skutkuje zielonym odznaczeniem statusu `Build Passing` na profilu repozytorium.

### B. Lokalny Stack Kontenerowy (`docker-compose.yml`)
* **Komponenty:**
  1. **Trino Coordinator** (port `8080`): Główny silnik SQL ze skonfigurowanym katalogiem Iceberga (`iceberg.properties`).
  2. **MinIO** (porty `9000` API, `9001` Konsola Web): Lokalna emulacja pamięci masowej AWS S3.
  3. **MinIO Client (`mc`)**: Automatycznie tworzy bucket `warehouse` na S3 przy starcie środowiska.
* **Uruchomienie:**
  Wystarczy jedna komenda: `docker compose up -d`, aby postawić lokalne środowisko Lakehouse i testować modele dbt bez konieczności płacenia za chmurę AWS.
