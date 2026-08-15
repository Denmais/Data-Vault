# Data Vault — Superstore ETL Pipeline

Учебный data engineering проект по построению хранилища данных в архитектуре **Data Vault** на основе датасета Superstore.

Проект демонстрирует полный путь данных: от исходного CSV-файла и генерации hash keys до формирования структуры Data Vault и загрузки данных в PostgreSQL.

![Data Vault schema](data.png)

## О проекте

В качестве источника используется датасет **Sample Superstore**, содержащий информацию о продажах, клиентах, товарах и способах доставки.

Исходный набор данных:

- **9 994 записи**
- **13 исходных признаков**
- данные о продажах, прибыли и скидках;
- география клиентов;
- категории и подкатегории товаров;
- сегменты покупателей;
- способы доставки.

Цель проекта — преобразовать плоский бизнес-датасет в структуру хранилища, основанную на принципах **Data Vault**.

## Архитектура

Пайплайн состоит из нескольких этапов:

```text
SampleSuperstore.csv
        │
        ▼
    loader.py
        │
        │  очистка и нормализация ключей
        │  генерация SHA-256 hash keys
        ▼
buisness_data.csv
        │
        ├──────────────► Hubs
        │                H_SALE
        │                H_CUSTOMER
        │                H_PRODUCT
        │                H_SHIPMENT
        │
        ├──────────────► Links
        │                L_SALE
        │                L_PRODUCT
        │                L_SHIPMENT
        │
        └──────────────► Satellites
                         S_CUSTOMER
                         S_DETAILS
        │
        ▼
   PostgreSQL
```

## Data Vault модель

### Hubs

**Hubs** содержат уникальные бизнес-сущности и их hash keys.

| Hub | Назначение |
|---|---|
| `H_SALE` | идентификаторы продаж |
| `H_CUSTOMER` | идентификаторы клиентов |
| `H_PRODUCT` | идентификаторы продуктов |
| `H_SHIPMENT` | идентификаторы способов/операций доставки |

### Links

**Links** описывают отношения между основными сущностями.

| Link | Связь |
|---|---|
| `L_SALE` | Sale ↔ Customer |
| `L_PRODUCT` | Sale ↔ Product |
| `L_SHIPMENT` | Sale ↔ Shipment |

### Satellites

**Satellites** предназначены для хранения описательных атрибутов сущностей и истории их изменения.

`S_CUSTOMER` хранит географические характеристики клиента:

- Postal Code
- Country
- Region
- State
- City

`S_DETAILS` содержит описательную информацию о продукте.

## Генерация Hash Keys

Для формирования ключей используется **SHA-256**.

Перед хешированием значения нормализуются:

```python
s.astype("string").str.strip().str.upper()
```

После этого несколько бизнес-атрибутов объединяются в одну строку:

```text
VALUE_1|VALUE_2|VALUE_3
```

и преобразуются в SHA-256 hash.

Такой подход позволяет получать детерминированные ключи независимо от последовательности загрузок данных.

Пример:

```python
df["Product_HK"] = make_bk_hash(
    df,
    ["Category", "Sub-Category"]
)
```

## ETL-процесс

### 1. Source

Исходные данные находятся в:

```text
SampleSuperstore.csv
```

### 2. Transformation

Скрипт:

```text
loader.py
```

загружает исходный CSV через `pandas`, нормализует бизнес-ключи и формирует hash keys для сущностей Data Vault.

Результат сохраняется в:

```text
buisness_data.csv
```

### 3. Data Vault schema

SQL-структура хранилища описана в:

```text
buisness.sql
```

Скрипт создаёт:

- schema PostgreSQL;
- Hub-таблицы;
- Link-таблицы;
- Satellite-таблицы;
- связи между сущностями.

### 4. Load

Скрипт:

```text
load_to_sql.py
```

формирует наборы данных для соответствующих Data Vault сущностей и загружает их в PostgreSQL через **SQLAlchemy**.

## Стек

- **Python**
- **Pandas**
- **SQL**
- **PostgreSQL**
- **SQLAlchemy**
- **psycopg2**
- **SHA-256 / hashlib**
- **Data Vault**

## Структура проекта

```text
Data-Vault/
│
├── SampleSuperstore.csv    # исходный датасет
├── buisness_data.csv       # данные после преобразования
│
├── loader.py               # preprocessing и генерация hash keys
├── load_to_sql.py          # загрузка данных в PostgreSQL
├── buisness.sql            # DDL Data Vault
│
├── data.png                # схема хранилища
└── README.md
```

## Локальный запуск

### 1. Клонировать репозиторий

```bash
git clone <repository-url>
cd Data-Vault
```

### 2. Создать виртуальное окружение

```bash
python -m venv .venv
```

Linux / macOS:

```bash
source .venv/bin/activate
```

Windows:

```bash
.venv\Scripts\activate
```

### 3. Установить зависимости

```bash
pip install pandas sqlalchemy psycopg2-binary
```

Если требуется автоматическая загрузка датасета через Kaggle:

```bash
pip install kagglehub
```

### 4. Подготовить данные

```bash
python loader.py
```

Будет создан файл:

```text
buisness_data.csv
```

с исходными данными и дополнительными hash keys.

### 5. Создать структуру БД

Выполнить SQL из:

```text
buisness.sql
```

в PostgreSQL.

### 6. Настроить подключение к PostgreSQL

Параметры подключения к базе данных рекомендуется хранить в **environment variables** или `.env`, а не непосредственно в исходном коде.

Пример:

```env
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/database
```

### 7. Загрузить данные

```bash
python load_to_sql.py
```

## Что демонстрирует проект

Проект показывает практическую работу с несколькими задачами Data Engineering:

- проектирование Data Vault;
- преобразование бизнес-данных в Hub / Link / Satellite модель;
- генерация детерминированных hash keys;
- preprocessing данных с Pandas;
- проектирование SQL-схемы;
- организация ETL-пайплайна;
- интеграция Python с PostgreSQL;
- подготовка данных для дальнейшей аналитики.

## Возможные улучшения

Следующие шаги развития проекта:

- вынести конфигурацию БД в `.env`;
- добавить `requirements.txt`;
- разделить ETL на отдельные extract / transform / load слои;
- добавить обработку дубликатов при повторной загрузке;
- реализовать инкрементальную загрузку;
- добавить `Record Source` и полноценную историю Satellite;
- добавить логирование;
- покрыть генерацию ключей unit-тестами;
- запускать PostgreSQL через Docker Compose;
- добавить orchestration через Airflow или Prefect;
- построить Business Vault и аналитические витрины поверх Raw Vault.

---

**Data Vault + Python + PostgreSQL**

Небольшой проект, демонстрирующий переход от плоского CSV-датасета к структурированному Data Vault хранилищу и ETL-процессу его наполнения.
