# Tarea 3 — Procesamiento de Datos con Apache Spark

**Curso:** Big Data (202016911) — Universidad Nacional Abierta y a Distancia
**Dataset:** Matrículas de la Escuela Nacional del Deporte
**Autor:** jesustfgo — jesustfgo@gmail.com

---

## 1. Descripción del problema

La Escuela Nacional del Deporte es una institución de educación superior en
Cali que forma profesionales del deporte y la salud. Cada semestre publica el
listado completo de matriculados para toda la institución y sus sedes.

La pregunta que responde este proyecto es: **¿cómo se distribuyen los
23.596 estudiantes matriculados entre programas, géneros, estratos y
procedencias, y qué trends revela esa distribución?**

El volumen y la estructura del dataset lo hacen candidato natural para
procesamiento distribuido, y la necesidad de responder preguntas sobre cohorts
que cambian cada semestre justifica un segundo escenario de procesamiento en
tiempo real sobre telemetría de sensores.

---

## 2. Estructura del repositorio

```
.
├── batch/
│   ├── fase1_batch.py          Análisis batch completo (EDA + 7 gráficos)
│   └── top_programa.py          Consulta aislada: programa más seleccionado
├── streaming/
│   ├── kafka_producer.py        Generador de datos en tiempo real
│   └── spark_streaming_consumer.py  Consumidor con ventanas de 1 minuto
├── datos/
│   └── ESTUDIANTES_...csv       Dataset original (2,7 MB, UTF-8)
├── resultados/
│   ├── reporte_batch.txt        Reporte de la ejecución batch
│   └── graficos/                Los 7 PNG del batch
├── run_batch.sh                 Ejecución del batch en Linux
├── run_streaming.sh             Ejecución de Kafka + Spark Streaming
├── requirements.txt
└── README.md
```

---

## 3. Dataset

23.596 registros × 10 columnas, codificación **UTF-8**, delimitador coma.

| Columna | Tipo | Descripción |
|---|---|---|
| `#` | double | Identificador de la fila |
| `PERÍODO` | string | Periodo académico (2023-1 … 2025-2) |
| `INSTITUCIÓN UNIVERSITARIA` | string | Institución o sede |
| `CÓDIGO PROGRAMA` | double | Código del programa (con decimales) |
| `PROGRAMA` | string | Nombre del programa |
| `SEMESTRE` | integer | Semestre cursado |
| `GÉNERO` | string | M / F |
| `ESTRATO` | integer | Estrato socioeconómico |
| `CIUDAD PROCEDENCIA` | string | Ciudad de origen |
| `DEPARTAMENTO PROCEDENCIA` | string | Departamento de origen |

---

## 4. Cómo ejecutar

### Requisitos

- Ubuntu Server 22.04 en máquina virtual (VirtualBox, puerto 2222 en SSH)
- Java 17
- Python 3.10 con PySpark 3.5.1 y matplotlib

```bash
pip install -r requirements.txt
```

### Batch

```bash
bash run_batch.sh
```

Genera `reporte_batch.txt` y 7 gráficos PNG. Tarda ~31 segundos.

### Consulta rápida del programa más seleccionado

```bash
python3 batch/top_programa.py
```

### Tiempo real (Kafka + Spark Structured Streaming)

```bash
bash run_streaming.sh
```

Abre la interfaz de Spark en <http://localhost:4040> para ver el Event
Timeline, donde cada bloque es un micro-lote de 20 segundos.

---

## 5. Resultados del batch

Universo: **23.596 estudiantes** · 6 periodos (2023-1 a 2025-2) · 0 filas descartadas.

### Top 5 programas

| Programa | Estudiantes | Sem. promedio |
|---|---:|---:|
| DEPORTE | 5.873 | 4,41 |
| FISIOTERAPIA | 5.746 | 5,44 |
| ADMINISTRACIÓN DE EMPRESAS | 3.153 | 4,24 |
| TECNOLOGÍA EN DEPORTE | 2.132 | 3,26 |
| TERAPIA OCUPACIONAL | 1.794 | 4,37 |

### Distribución por género

| Género | Estudiantes | % |
|---|---:|---:|
| M | 12.748 | 54,03 |
| F | 10.848 | 45,97 |

### Los 7 gráficos

| Archivo | Qué muestra |
|---|---|
| `g1_matriculas_por_periodo.png` | Evolución del universo en 6 periodos |
| `g2_top_programas.png` | Concentración de la matrícula |
| `g3_genero.png` | Brecha de género global |
| `g4_estrato_por_genero.png` | Composición socioeconómica |
| `g5_top_ciudades.png` | Concentración geográfica |
| `g6_semestre_promedio_programa.png` | Avance académico por programa |
| `g7_brecha_genero_programa.png` | Sesgo de género por programa |

---

## 6. Hallazgos

**1. El 99,33% del universo es una sola institución.** 23.437 de 23.596
registros son de la Escuela Nacional del Deporte; las 159 restantes son del
CERES Salto Afro Guachene. Cualquier hallazgo sobre "la institución" describe
una sola institución, no el sector.

**2. La brecha de género es segmentada, no gradual.** No hay espectro: hay dos
bloques. Los programas de rendimiento deportivo son mayoritariamente
masculinos (DEPORTE 20,8% mujeres, TECNOLOGÍA EN DEPORTE 23,5%) y los de salud y ocupacional son mayoritariamente
femeninos (NUTRICIÓN Y DIETÉTICA 76,0%, TERAPIA OCUPACIONAL 74,0%). Administración
de Empresas es la excepción: −2,9% de brecha.

**3. La matrícula crece, pero no se diversifica.** De 2023-1 (3.661) a 2025-2
(4.405) hay +20,4%, pero el 24,9% sigue siendo un solo programa. El crecimiento
proviene de más estudiantes del mismo perfil, no de diversificación.

**4. El estrato 2 predomina.** 9.749 de 23.596 (41,3%). Solo 1.067 están en
estratos 4 a 6 (4,5%). Además 295 registros (1,25%) no tienen estrato.

**5. La concentración es departamental.** Cali aporta 14.258 estudiantes y el
Valle del Cauca 16.353. El 0,65% no tiene ciudad registrada.

---

## 7. Problemas encontrados y soluciones aplicadas

**1. Categorías fantasma por tilde.** El dataset trae `ADMINISTRACION DE
EMPRESAS` y `ADMINISTRACIÓN DE EMPRESAS` como registros separados, igual que
`TECNOLOGIA EN DEPORTE` / `TECNOLOGÍA EN DEPORTE`. Contadas sin normalizar,
Spark las trata como dos programas distintos y el Top 5 sale mal. Se resolvió
con `translate()` para eliminar tildes y `regexp_replace` para colapsar
espacios. Administración de Empresas pasó de 1.887 a **3.153** estudiantes y
Tecnología en Deporte de 1.291 a **2.132**.

**2. PySpark incompatible con Python 3.13.** En el host Windows, PySpark 3.5.7
con Python 3.13 o 3.14 provoca el fallo `Python worker exited unexpectedly`.
El análisis se ejecuta en la VM con Python 3.10.12 y PySpark 3.5.1.

**3. `inferSchema` infiere mal el identificador.** Spark tipa `#` y
`CÓDIGO PROGRAMA` como `double`, no como entero, y los muestra con puntos de
miles (`53.212`). Para cruzar por código en la fase de streaming conviene
declarar el esquema explícito.

**4. `MISSING_GROUP_BY` al calcular porcentajes.** Una función de agregación
(`count`) dentro de `withColumn()` sobre un DataFrame ya agrupado lanza
`AnalysisException`. El porcentaje debe calcularse sobre la columna agregada.

**5. Codificación al transferir archivos.** Subir el script con
`Get-Content | ssh cat` en PowerShell corrompe los acentos (`GÉNERO` queda
ilegible) y la columna nunca se renombra. Hay que transferir por SFTP binario.

---

## 8. Procesamiento en tiempo real

| Componente | Versión / detalle |
|---|---|
| Kafka | 3.6.2 — topic `sensor_data`, puerto 9092 |
| Zookeeper | puerto 2181 |
| Spark | 3.5.8 — Structured Streaming |
| Interfaz | <http://localhost:4040> |

Configuración del consumidor: ventanas de **1 minuto**, watermark de **2
minutos**, trigger de **20 segundos**, 5 sensores emite 1 lectura por segundo.

La lógica de `window()` es que Spark no espera a terminar el minuto para
empezar: acumula datos según llegan y cada vez que la ventana se cierra emite
los resultados. El trigger de 20 segundos solo define cada cuánto revisa; el
CSV crece cada minuto sin relanzar nada.

---

## 9. Licencia y autoría

Uso académico. El código es original y fue verificado ejecutándose sobre la
máquina virtual BIGDATA2.

**Autor:** jesustfgo
**Correo:** jesustfgo@gmail.com

<!-- Si el trabajo es grupal, agregue los demás integrantes aquí. -->