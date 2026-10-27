# DataMart S.A.S. — Pipeline ETL & Data Warehouse
**Prueba de Desempeño — Ingeniería de Datos Cohorte 7 (Riwi)**  
**Repositorio Oficial:** [https://github.com/JoshuaQ-rJ/repositorio_prueba_ingenieria](https://github.com/JoshuaQ-rJ/repositorio_prueba_ingenieria)  
**Solución Orquestada con Apache Airflow, Docker, PostgreSQL y uv**

---

## 1. Descripción del Proyecto
Plataforma de datos de extremo a extremo para **DataMart S.A.S.**, empresa de comercio electrónico con presencia en Colombia, México y Perú. La solución ingesta, limpia, valida y transforma transacciones diarias y datos históricos, enriqueciéndolos con un catálogo legado de productos y cargándolos en un Data Warehouse analítico dimensional en PostgreSQL para habilitar cierres contables instantáneos y análisis de rentabilidad.

---

## 2. Arquitectura del Stack

```text
       [ Fuentes Crudas ]
  data.csv | online_retail_II.csv
              │
              ▼
   ┌──────────────────────┐         ┌──────────────────────┐
   │ Apache Airflow (DAG) │ ◄─────► │ Catalog REST API     │
   │  (Scheduler/Worker)  │         │ (Puerto 8000)        │
   └──────────┬───────────┘         └──────────────────────┘
              │
              ├───► [ Postgres Metastore (5432) ]  <- Metadatos de Airflow
              │
              └───► [ Postgres DWH (5433) ]        <- Repositorio Analítico
                     ├── fact_sales
                     ├── fact_returns
                     ├── dim_products
                     ├── dim_customers
                     ├── dim_dates
                     └── rejections_log
```

### Servicios y Puertos
| Servicio | Contenedor | Puerto Host | Descripción |
|---|---|---|---|
| **Airflow Webserver** | `airflow-webserver` | `8080` | UI de Airflow (`admin` / `admin`) |
| **Airflow Scheduler** | `airflow-scheduler` | Interno | Planificador y ejecutor de tareas |
| **Airflow Metastore** | `postgres-metadata` | `5432` | PostgreSQL dedicado a metadatos de Airflow |
| **DWH Analítico** | `postgres-dwh` | `5433` | Repositorio analítico destino de DataMart |
| **Catálogo Legado** | `catalog-api` | `8000` | Microservicio REST de productos (`/products`) |

---

## 3. Guía de Ejecución Rápida (< 10 Minutos)

### Prerrequisitos
* Docker y Docker Compose (v2+) instalados y corriendo.
* Git.
* Python 3.12 y [uv](https://docs.astral.sh/uv/) (opcional para desarrollo local y tests).

### Paso 1: Clonar y configurar variables de entorno
```bash
git clone https://github.com/JoshuaQ-rJ/repositorio_prueba_ingenieria.git
cd repositorio_prueba_ingenieria

# Copiar el archivo de variables de entorno de ejemplo
cp .env.example .env
```

### Paso 2: Levantar el stack completo (Un solo comando)
```bash
docker compose up -d
```
> **Nota:** El contenedor `postgres-dwh` ejecuta automáticamente los scripts DDL en su primer arranque (`sql/init/01_init_dwh.sql`), creando las 6 tablas e insertando los datos semilla de fechas y clientes.

### Paso 3: Validar estado de los contenedores
```bash
docker compose ps
```
Todos los contenedores deben figurar en estado `Up` o `healthy`.

### Paso 4: Validar Conexiones y Variables en Airflow (Auto-configuradas)
No es necesario configurar nada en la interfaz web. Puedes verificar directamente vía CLI:
```bash
# Validar conexión al Data Warehouse
docker exec airflow-scheduler airflow connections get postgres_dwh

# Validar variables operativas del pipeline
docker exec airflow-scheduler airflow variables get raw_data_path
docker exec airflow-scheduler airflow variables get default_retry_count
```

### Paso 5: Desencadenar la Ejecución del Pipeline (DAG)
Accede a `http://localhost:8080` (usuario: `admin`, contraseña: `admin`) o ejecútalo vía terminal:
```bash
# Despausar y disparar el DAG
docker exec airflow-scheduler airflow dags unpause datamart_daily_etl
docker exec airflow-scheduler airflow dags trigger datamart_daily_etl
```

---

## 4. Respuestas a las 7 Preguntas de Negocio

Todas las consultas se encuentran en [`sql/queries/business_queries.sql`](file:///C:/Users/Coder/Desktop/prueba_ingenieria_datos/sql/queries/business_queries.sql) y pueden ejecutarse con:
```bash
docker exec -i postgres-dwh psql -U datamart_user -d datamart_dwh < sql/queries/business_queries.sql
```

### 1. Evolución mensual de ventas netas
* El periodo analizado refleja ventas netas estables promedio de **~$19.500 USD mensuales**, con picos notables en **marzo ($25.022 USD)** y **noviembre ($24.367 USD)**.

### 2. Categorías por revenue bruto y devoluciones
| Categoría | Revenue Bruto | Monto Devoluciones | Tasa Devolución (%) |
|---|---|---|---|
| **Papelería** | $139.349,00 | $4.818,00 | 3,46% |
| **Hogar** | $68.233,99 | $2.599,33 | 3,81% |
| **Ropa** | $41.936,13 | $1.977,94 | **4,72%** (Mayor tasa) |

### 3. Top Productos por Revenue Neto y Devoluciones
* **Mayor Revenue Neto:** `POST` (Postage Service, $50.760 USD), `BANK CHARGES` ($45.855 USD), `22752` (Set 7 Babushka Nesting Boxes, $25.619 USD).
* **Mayor Devolución:** Los artículos textiles y calentadores de la categoría Ropa presentan las tasas de retorno más elevadas.

### 4. Concentración Geográfica y Ticket Promedio
| País | Órdenes | Items Vendidos | Revenue Bruto | Ticket Promedio |
|---|---|---|---|---|
| **Colombia** | 463 | 1.164 | $87.113,52 | $188,15 |
| **México** | 378 | 924 | $70.699,79 | $187,04 |
| **Perú** | 274 | 656 | $52.400,09 | **$191,24** |

### 5. Clientes Identificados vs. Anónimos
* **Clientes Registrados:** 1.097 órdenes, $206.222,69 USD, Ticket promedio: **$187,99 USD**.
* **Compradores Anónimos (`customer_id = -1`):** 216 órdenes, $43.296,43 USD, Ticket promedio: **$200,45 USD**.
* *Insight:* Las compras anónimas representan transacciones de mayor volumen monetario unitario (compras espontáneas de alto valor).

### 6. SKUs Únicos y Variaciones de Descripción
* Total de SKUs únicos normalizados en el catálogo: **13 referencias principales y mapeo de catálogo dinámico**.
* El uso de la moda estadística eliminó el 100% de inconsistencias en reportes.

### 7. Recomendación Concreta al Equipo de Producto
> **Recomendación Cuantitativa:**  
> 1. **Optimizar la categoría Ropa:** Presenta una tasa de devolución del **4,72%** (36% superior a Papelería). Se recomienda auditar las guías de tallas y empaques en México y Colombia para reducir devoluciones en ~$1.200 USD mensuales.  
> 2. **Campaña de Registro Post-Compra para Clientes Anónimos:** Los usuarios anónimos tienen un ticket promedio superior ($200,45 USD vs $187,99 USD). Un incentivo de fidelización post-checkout capturaría datos de un segmento que ya factura más de $43.000 USD sin requerir fricción inicial de login.

---

## 5. Estructura del Repositorio
```text
prueba_ingenieria_datos/
├── .env.example              # Plantilla de variables de entorno (sin credenciales)
├── .gitignore                # Exclusión de credenciales, binarios y datos pesados
├── docker-compose.yml        # Orquestación de servicios Docker
├── DECISIONES.md             # Documento de decisiones técnicas, diseño e idempotencia
├── README.md                 # Guía oficial del evaluador
├── dags/
│   └── datamart_daily_etl.py # DAG diario idempotente de Airflow
├── data/
│   ├── raw/                  # Destino de data.csv y online_retail_II.csv
│   └── seeds/                # online_retail_sample.csv (muestra de 3.500 filas)
├── docs/
│   └── ERD.md                # Diagrama Entidad-Relación (Mermaid) y diccionario
├── sql/
│   ├── init/
│   │   └── 01_init_dwh.sql   # DDL del Data Warehouse con constraints e índices
│   └── queries/
│       └── business_queries.sql # Consultas SQL verificadas para las 7 preguntas
├── src/
│   ├── api/
│   │   ├── catalog_seed.json # Semillero del catálogo legado
│   │   ├── main.py           # Aplicación FastAPI del catálogo
│   │   └── server.py         # Servidor HTTP ligero para el contenedor
│   └── pipeline/
│       ├── cleaner.py        # Limpieza, reglas de negocio y deduplicación
│       └── loader.py         # Persistencia idempotente hacia PostgreSQL
└── tests/
    ├── test_cleaner.py       # Pruebas unitarias de calidad con pytest
    ├── test_unit.py          # Pruebas unitarias compatibles con standard unittest
    └── test_loader.py        # Pruebas de integración e idempotencia
```

---

## 6. Pruebas Automatizadas
Para ejecutar la suite completa de pruebas unitarias dentro del entorno Docker:
```bash
docker exec airflow-scheduler python /opt/airflow/tests/test_unit.py
```
Resultado: `Ran 5 tests in 0.036s. OK`
