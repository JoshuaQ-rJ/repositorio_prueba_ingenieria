# Documento de Decisiones Técnicas y de Negocio
**Proyecto:** DataMart S.A.S. — Pipeline ETL & Data Warehouse  
**Rol:** Data Engineer / Architect  
**Evaluador:** Riwi — Cohorte 7  

---

## 1. Diseño del Modelo del Repositorio Analítico (Data Warehouse)

### Justificación de la Arquitectura
Para responder a los dolores de negocio de DataMart S.A.S. (cierre financiero lento, falta de visibilidad sobre pérdidas, formatos disparatados y mezcla de devoluciones con ventas), se adoptó un **Modelo Dimensional Kimball en Esquema Estrella (Star Schema)** en PostgreSQL, complementado con una capa de auditoría y cuarentena.

### Estructura de Tablas
1. **`fact_sales` (Hecho de Ventas Válidas):**
   * Grano: Una línea de ítem vendido con cantidad estrictamente $> 0$ y precio unitario $> 0$.
   * Métricas aditivas: `quantity`, `unit_price`, `gross_revenue` ($quantity \times unit\_price$) y `net_revenue` ($gross\_revenue - devoluciones\_diarias$).
   * Claves foráneas: `stock_code` ($\rightarrow$ `dim_products`), `customer_id` ($\rightarrow$ `dim_customers`), `date_key` ($\rightarrow$ `dim_dates`).
   * Constraint de idempotencia: `UNIQUE (invoice_no, stock_code, invoice_date_utc, quantity)`.

2. **`fact_returns` (Hecho de Devoluciones y Ajustes):**
   * Grano: Una línea de ítem cancelado o devuelto con cantidad $< 0$.
   * Métricas: `return_amount` ($|quantity| \times unit\_price$).
   * Separar devoluciones en una tabla dedicada evita distorsionar el volumen bruto de órdenes y permite a finanzas realizar la conciliación instantánea ($Ventas\_Netas = Ventas\_Brutas - Devoluciones$).

3. **`dim_products` (Dimensión Producto):**
   * Atributos: `stock_code` (clave natural), `canonical_name`, `category`, `supplier_country`, `is_active`.
   * Enriquecido con las 5 categorías oficiales: *Electrónica, Hogar, Ropa, Deportes, Papelería*.

4. **`dim_customers` (Dimensión Cliente):**
   * Atributos: `customer_id`, `customer_code`, `country`, `is_anonymous`.
   * Incluye el registro centinela `customer_id = -1` (`ANONYMOUS`, `UNKNOWN`) para preservar la integridad referencial en transacciones sin cliente identificado.

5. **`dim_dates` (Dimensión Calendario UTC):**
   * Atributos: `date_key` (YYYYMMDD), `full_date`, `year`, `month`, `day`, `quarter`, `day_of_week`.
   * Pre-poblada con 13.149 días calendario para joins analíticos ultrarrápidos.

6. **`rejections_log` (Log de Cuarentena y Auditoría):**
   * Columnas: `rejection_id`, `source_file`, `raw_record_id`, `rejection_reason`, `rejected_at`, `payload` (JSONB con la fila original completa).
   * Almacena cualquier fila descartada por violación de reglas de negocio para auditoría sin detener el pipeline.

---

## 2. Resolución de Casos Ambiguos y Reglas de Negocio

### Caso 1: Transacciones sin Customer ID
* **Decisión:** **Incluirlas en el repositorio analítico asignándoles el identificador sintético `customer_id = -1` (`is_anonymous = TRUE`).**
* **Justificación de Negocio:** Excluir estas órdenes habría eliminado **\$43.296,43 USD** en ventas (un 17,3% del revenue total de la muestra). Además, el análisis reveló que las compras anónimas presentan un **ticket promedio mayor (\$200,45 USD vs \$187,99 USD)** que el de clientes registrados, lo que representa un segmento de compradores espontáneos de alto valor que no debe pasar desapercibido.

### Caso 2: Variaciones de Descripción para un Mismo Código de Producto
* **Decisión:** **Asignación del nombre canónico por Moda Estadística (frecuencia máxima histórica).**
* **Justificación:** Algunos SKUs tienen variantes como `"WHITE HANGING HEART T-LIGHT HOLDER"`, `"white heart holder"`, o erratas tipográficas. El pipeline calcula la descripción más utilizada en el histórico para cada SKU y la fija en `dim_products`, preservando la consistencia en reportes y BI.

### Caso 3: Solapamiento entre Fuentes (`data.csv` vs `online_retail_II.csv`)
* **Decisión:** **Deduplicación por clave compuesta `(invoice_no, stock_code, timestamp_utc, quantity)` con prioridad a la fuente diaria.**
* **Justificación:** Si un registro existe en ambas fuentes, se prioriza el volcado operativo diario (`data.csv`) sobre el lote histórico extendido (`online_retail_II.csv`), garantizando la frescura y exactitud de los datos.

### Caso 4: Precio Unitario $\le 0$
* **Decisión:** **Rechazo total y almacenamiento en `rejections_log` con motivo `INVALID_UNIT_PRICE`.**
* **Justificación:** Una venta con precio cero o negativo no constituye una transacción comercial válida de comercio electrónico (corresponde a errores de captura, muestras o promociones mal etiquetadas). Al registrarse en el log con su payload original en JSONB, el equipo de auditoría puede investigarlas sin corromper las métricas financieras.

### Caso 5: Cantidad $\le 0$
* **Decisión:** **Tratamiento exclusivo como devolución/ajuste en `fact_returns`.**
* **Justificación:** Permite auditar la tasa de devolución por categoría y SKU sin mezclar signos negativos en la tabla principal de ventas, facilitando cálculos aditivos limpios en herramientas BI.

---

## 3. Garantía de Idempotencia del DAG

La idempotencia exige que ejecutar el pipeline varias veces con los mismos datos produzca exactamente el mismo resultado en el repositorio analítico. En DataMart, se garantiza mediante tres capas:

1. **Constraints de Unicidad en Base de Datos:**
   * `fact_sales` posee `CONSTRAINT uq_fact_sales_item UNIQUE (invoice_no, stock_code, invoice_date_utc, quantity)`.
   * `fact_returns` posee `CONSTRAINT uq_fact_returns_item UNIQUE (invoice_no, stock_code, return_date_utc, quantity)`.
2. **Cláusula `ON CONFLICT DO NOTHING` en la Inserción:**
   * Si una tarea falla a mitad de camino o el DAG se re-ejecuta manualmente, los registros ya insertados se ignoran sin lanzar errores ni duplicar filas.
3. **Validación Experimental de Idempotencia:**
   * Se ejecutó el pipeline dos veces de forma consecutiva (Run 1 y Run 2).
   * **Resultado:** El conteo de ventas permaneció inmutable en **3.251 filas** y el de devoluciones en **242 filas**, confirmando 100% de determinismo e idempotencia.

---

## 4. Arquitectura de Despliegue Docker y Auto-configuración

* **Cero intervención manual:** Toda la infraestructura levanta con `docker compose up -d`.
* **Aprovisionamiento nativo de Airflow:**
  * La conexión al DWH se inyecta como variable de entorno: `AIRFLOW_CONN_POSTGRES_DWH=postgresql://datamart_user:datamart_secure_pass@postgres-dwh:5432/datamart_dwh`.
  * Los parámetros operativos se inyectan como variables: `AIRFLOW_VAR_RAW_DATA_PATH=/opt/airflow/data/raw` y `AIRFLOW_VAR_DEFAULT_RETRY_COUNT=2`.
* **Microservicio REST de Catálogo:** Se implementó el servicio `catalog-api` en el puerto `8000` con endpoints `/health`, `/products` y `/products/{code}`.
