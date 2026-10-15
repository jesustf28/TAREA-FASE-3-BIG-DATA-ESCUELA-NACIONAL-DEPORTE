#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FASE 1 - ANALISIS ESTATICO EN BATCH (PySpark + visualizacion)
Dataset institucional: Escuela Nacional del Deporte
Universidad Nacional Abierta y a Distancia - Big Data (202016911)

Cumple el punto 3 "Implementacion en Spark -> Procesamiento en batch":
  - Cargar el conjunto de datos desde la fuente original
  - Limpieza, transformacion y EDA con DataFrames
  - Visualizar los resultados del procesamiento en batch   <-- este script

Salida: ~/Escritorio/BIGDATA 3/fase1_batch/
Ejecucion 100% CLI, sin interfaz grafica.
"""

import os
import io
import sys
import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")                      # render headless, sin pantalla
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
# CONFIGURACION
# =============================================================================
HOME = os.path.expanduser("~")
AQUI = os.path.dirname(os.path.abspath(__file__))          # .../batch
RAIZ = os.path.dirname(AQUI)                             # .../Tarea3-BigData-UNAD

# Prioridad de la carpeta de salida:
#   1) variable de entorno SALIDA_DIR
#   2) resultados/ dentro del repo, si el repo esta completo
#   3) ~/Escritorio/BIGDATA 3/fase1_batch
if os.environ.get("SALIDA_DIR"):
    OUT_DIR = os.path.abspath(os.environ["SALIDA_DIR"])
elif os.path.isdir(os.path.join(RAIZ, "datos")):
    OUT_DIR = os.path.join(RAIZ, "resultados")
else:
    OUT_DIR = os.path.join(HOME, "Escritorio", "BIGDATA 3", "fase1_batch")

GRAF_DIR = os.path.join(OUT_DIR, "graficos")
REPORT = os.path.join(OUT_DIR, "reporte_batch.txt")

NOMBRE_CSV = "ESTUDIANTES_MATRICULADOS_EN_LA_ESCUELA_NACIONAL_DEL_DEPORTE_20261014"
RUTAS_BUSQUEDA = [
    os.path.join(RAIZ, "datos", NOMBRE_CSV),           # dentro del repo (prioritario)
    os.path.join(HOME, "Descargas", NOMBRE_CSV),
    os.path.join(HOME, NOMBRE_CSV),
    os.path.join(HOME, "Tarea3-BigData-UNAD", "datos", NOMBRE_CSV),
]

MAPEO = {
    "#": "id", "PERÍODO": "periodo",
    "INSTITUCIÓN UNIVERSITARIA": "institucion",
    "CÓDIGO PROGRAMA": "codigo_programa", "PROGRAMA": "programa",
    "SEMESTRE": "semestre", "GÉNERO": "genero", "ESTRATO": "estrato",
    "CIUDAD PROCEDENCIA": "ciudad", "DEPARTAMENTO PROCEDENCIA": "departamento",
}
COLS_TEXTO = ["periodo", "institucion", "programa", "genero", "ciudad", "departamento"]
COLS_CLAVE = ["id", "programa"]

# Paleta sobria, legible en video
AZUL, VERDE, NARANJA, ROJO, GRIS = "#2c5f8d", "#3d8b5f", "#d98c2b", "#b8453c", "#6b7280"
LOG = []


def emit(line=""):
    print(line)
    LOG.append(str(line))


def regla(t):
    emit("")
    emit("=" * 78)
    emit(t)
    emit("=" * 78)


def tabla(df, max_filas=20):
    filas = df.limit(max_filas).collect()
    if not filas:
        return ["(sin registros)"]
    cols = df.columns
    anchos = [len(str(c)) for c in cols]
    celdas = []
    for f in filas:
        fila = []
        for i, v in enumerate(f):
            s = "NULL" if v is None else str(v)
            anchos[i] = max(anchos[i], len(s))
            fila.append(s)
        celdas.append(fila)
    sep = "+" + "+".join("-" * (a + 2) for a in anchos) + "+"
    out = [sep,
           "| " + " | ".join(str(c).ljust(anchos[i]) for i, c in enumerate(cols)) + " |",
           sep]
    for fila in celdas:
        out.append("| " + " | ".join(fila[i].ljust(anchos[i]) for i in range(len(cols))) + " |")
    out.append(sep)
    out.append(f"({df.count()} filas en total)")
    return out


def guardar(fig, nombre, titulo):
    ruta = os.path.join(GRAF_DIR, nombre)
    fig.savefig(ruta, dpi=110, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    kb = os.path.getsize(ruta) / 1024
    emit(f"  [PNG] {nombre:38} {kb:7.1f} KB   <- {titulo}")


def normalizar_texto(col):
    """Quita tildes y colapsa espacios, para que 'ADMINISTRACION DE EMPRESAS' y
    'ADMINISTRACIÓN DE EMPRESAS' se consolidate en UNA sola categoria.
    Sin esto Spark las cuenta como dos programas distintos."""
    sin_tilde = F.expr(
        "translate({c}, 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN')".format(c=col))
    return F.trim(F.regexp_replace(sin_tilde, r"\s+", " "))


def main():
    inicio = datetime.datetime.now()

    # ---------------------------------------------------------------- PASO 1
    regla("PASO 1 - CREACION DEL DIRECTORIO DE SALIDA")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(GRAF_DIR, exist_ok=True)
    emit(f"Carpeta      : {OUT_DIR}")
    emit(f"Graficos     : {GRAF_DIR}")
    emit(f"Reporte      : {REPORT}")

    ruta_csv = None
    for r in RUTAS_BUSQUEDA:
        for cand in (r, r + ".csv", r + ".txt"):
            if os.path.isfile(cand):
                ruta_csv = cand
                break
        if ruta_csv:
            break
    if not ruta_csv:
        emit("[ERROR] Dataset no encontrado.")
        sys.exit(1)
    emit("")
    emit(f"Dataset: {ruta_csv}")
    emit(f"Tamano : {os.path.getsize(ruta_csv):,} bytes")

    spark = (SparkSession.builder
             .appName("Fase1-Batch-EDA-ENDN")
             .master("local[*]")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------------- PASO 2a
    regla("PASO 2a - CARGA Y ESQUEMA")
    df_crudo = (spark.read
                .option("header", "true").option("delimiter", ",")
                .option("encoding", "UTF-8").option("inferSchema", "true")
                .option("mode", "PERMISSIVE").option("quote", '"')
                .csv(ruta_csv))
    total_crudo = df_crudo.count()
    emit(f"df.count()    : {total_crudo:,}")
    emit(f"Columnas      : {len(df_crudo.columns)}")
    emit("")
    emit("printSchema():")
    emit(df_crudo._jdf.schema().treeString())

    # ---------------------------------------------------------------- PASO 2b
    regla("PASO 2b - LIMPIEZA DE DATOS")
    df_r = df_crudo
    for o, n in MAPEO.items():
        if o in df_r.columns:
            df_r = df_r.withColumnRenamed(o, n)
    emit(f"Columnas renormal: {df_r.columns}")
    emit("")

    emit("Nulos por columna (ANTES):")
    for c in df_r.columns:
        emit(f"  {c:20} {df_r.filter(F.col(c).isNull()).count():>8,}")
    emit("")

    df_l = df_r
    for c in COLS_TEXTO:
        if c in df_l.columns:
            df_l = df_l.withColumn(c, F.trim(F.col(c)))
            df_l = df_l.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))
    if "genero" in df_l.columns:
        df_l = df_l.withColumn("genero", F.upper(F.regexp_replace(F.col("genero"), "[^A-Za-z]", "")))
    # Normalizacion de categorias: consolida variantes con/sin tilde
    for c in ["programa", "ciudad", "departamento", "institucion"]:
        if c in df_l.columns:
            df_l = df_l.withColumn(c, normalizar_texto(c))
    total_pre = df_l.count()
    df_l = df_l.dropna(subset=COLS_CLAVE)
    total_limpio = df_l.count()
    df_l = df_l.dropDuplicates()
    total_final = df_l.count()
    safe = total_final if total_final > 0 else 1

    emit("Balance de la limpieza:")
    emit(f"  Filas crudas                : {total_crudo:>8,}")
    emit(f"  Tras limpiar textos         : {total_pre:>8,}")
    emit(f"  Tras descartar claves nulas : {total_limpio:>8,}")
    emit(f"  Tras eliminar duplicados    : {total_final:>8,}")
    emit(f"  Descartadas                 : {total_crudo - total_final:>8,}")

    # ---------------------------------------------------------------- PASO 2c
    regla("PASO 2c - ANALISIS EXPLORATORIO (EDA)")

    emit("")
    emit("1) TOTAL DE ESTUDIANTES MATRICULADOS (limpios)")
    emit(f"   {total_final:,}")

    top5 = (df_l.groupBy("programa")
            .agg(F.count(F.lit(1)).alias("estudiantes"),
                 F.round(F.avg(F.col("semestre").cast("double")), 2).alias("semestre_prom"))
            .orderBy(F.desc("estudiantes")).limit(5))
    emit("")
    emit("2) TOP 5 PROGRAMAS ACADEMICOS")
    for l in tabla(top5):
        emit(l)

    dist_genero = (df_l.groupBy("genero").agg(F.count(F.lit(1)).alias("estudiantes"))
                   .withColumn("porcentaje", F.round(F.col("estudiantes") * 100.0 / safe, 2))
                   .orderBy(F.desc("estudiantes")))
    emit("")
    emit("3) DISTRIBUCION POR GENERO")
    for l in tabla(dist_genero):
        emit(l)

    dist_periodo = (df_l.groupBy("periodo").agg(F.count(F.lit(1)).alias("estudiantes"))
                    .orderBy(F.desc("estudiantes")))
    emit("")
    emit("4) DISTRIBUCION POR PERIODO")
    for l in tabla(dist_periodo):
        emit(l)

    dist_estrato = (df_l.groupBy("estrato").agg(F.count(F.lit(1)).alias("estudiantes"))
                    .orderBy(F.desc("estudiantes")))
    emit("")
    emit("5) DISTRIBUCION POR ESTRATO")
    for l in tabla(dist_estrato):
        emit(l)

    inst = (df_l.groupBy("institucion").agg(F.count(F.lit(1)).alias("estudiantes"))
            .orderBy(F.desc("estudiantes")))
    emit("")
    emit("6) INSTITUCIONES PRESENTES")
    for l in tabla(inst):
        emit(l)

    top_ciudad = (df_l.groupBy("ciudad", "departamento")
                  .agg(F.count(F.lit(1)).alias("estudiantes"))
                  .orderBy(F.desc("estudiantes")).limit(5))
    emit("")
    emit("7) TOP 5 CIUDADES DE PROCEDENCIA")
    for l in tabla(top_ciudad):
        emit(l)

    # ---- Brecha de genero por programa (corte analitico) ------------------
    brecha = (df_l.groupBy("programa", "genero").agg(F.count(F.lit(1)).alias("n"))
              .groupBy("programa")
              .agg(F.sum(F.when(F.col("genero") == "M", F.col("n")).otherwise(0)).alias("hombres"),
                   F.sum(F.when(F.col("genero") == "F", F.col("n")).otherwise(0)).alias("mujeres"))
              .withColumn("total", F.col("hombres") + F.col("mujeres"))
              .filter(F.col("total") > 0)
              .withColumn("pct_mujeres", F.round(F.col("mujeres") * 100.0 / F.col("total"), 2))
              .withColumn("brecha", F.round(((F.col("hombres") - F.col("mujeres")) * 100.0
                                             / F.col("total")), 2))
              .orderBy(F.desc("total")).limit(8))
    emit("")
    emit("8) BRECHA DE GENERO POR PROGRAMA  (positiva = mas hombres)")
    for l in tabla(brecha):
        emit(l)

    # ---- Semestre promedio por programa -----------------------------------
    sem_prog = (df_l.groupBy("programa")
                .agg(F.round(F.avg(F.col("semestre").cast("double")), 2).alias("semestre_prom"),
                     F.count(F.lit(1)).alias("estudiantes"))
                .filter(F.col("semestre_prom").isNotNull())
                .orderBy(F.desc("semestre_prom")).limit(8))
    emit("")
    emit("9) SEMESTRE PROMEDIO POR PROGRAMA")
    for l in tabla(sem_prog):
        emit(l)

    # ---------------------------------------------------------------- PASO 3
    regla("PASO 3 - VISUALIZACION DE LOS RESULTADOS (batch)")
    emit("")
    emit("Generando graficos PNG en: " + GRAF_DIR)
    emit("")

    plt.rcParams.update({"font.size": 9, "axes.titlesize": 11,
                         "axes.titleweight": "bold", "figure.facecolor": "white"})

    def pares(df, n):
        r = df.limit(n).collect()
        return [x[0] for x in r], [x[1] for x in r]

    # g1 matriculas por periodo
    labs, vals = pares(dist_periodo.orderBy("periodo"), 12)
    fig, ax = plt.subplots(figsize=(9, 4))
    b = ax.bar(labs, vals, color=AZUL)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g1. Matriculas por periodo academico")
    ax.set_ylabel("Estudiantes")
    ax.set_ylim(0, max(vals) * 1.15)
    plt.xticks(rotation=30, ha="right")
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g1_matriculas_por_periodo.png", "evolucion del universo en 6 periodos")

    # g2 top programas
    labs, vals = pares(top5, 8)
    fig, ax = plt.subplots(figsize=(9, 4))
    b = ax.barh(labs[::-1], vals[::-1], color=VERDE)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g2. Top 5 programas con mas estudiantes")
    ax.set_xlabel("Estudiantes")
    ax.grid(axis="x", alpha=.3)
    guardar(fig, "g2_top_programas.png", "concentracion de la matricula")

    # g3 genero
    labs, vals = pares(dist_genero, 5)
    fig, ax = plt.subplots(figsize=(6, 4))
    b = ax.bar(labs, vals, color=[AZUL, NARANJA][:len(labs)])
    ax.bar_label(b, padding=2, fontsize=9)
    ax.set_title("g3. Distribucion por genero")
    ax.set_ylabel("Estudiantes")
    ax.set_ylim(0, max(vals) * 1.15)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g3_genero.png", "brecha de genero global")

    # g4 estrato apilado por genero
    eg_base = (df_l.groupBy("estrato")
             .agg(F.sum(F.when(F.col("genero") == "M", 1).otherwise(0)).alias("M"),
                  F.sum(F.when(F.col("genero") == "F", 1).otherwise(0)).alias("F"))
             .orderBy("estrato"))
    eg = eg_base
    cols_eg = [c for c in eg.columns if c != "estrato"]
    filas_eg = eg.orderBy("estrato").collect()
    estratos = [str(r["estrato"]) for r in filas_eg]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    base = [0] * len(filas_eg)
    for col, color in zip(cols_eg, [AZUL, NARANJA]):
        vals_c = [r[col] or 0 for r in filas_eg]
        ax.bar(estratos, vals_c, bottom=base, label=col, color=color)
        base = [b + v for b, v in zip(base, vals_c)]
    ax.set_title("g4. Estrato socioeconómico por genero")
    ax.set_xlabel("Estrato"); ax.set_ylabel("Estudiantes")
    ax.legend(title="Genero"); ax.grid(axis="y", alpha=.3)
    guardar(fig, "g4_estrato_por_genero.png", "composicion socioeconomica")

    # g5 top ciudades
    labs, vals = pares(top_ciudad, 8)
    fig, ax = plt.subplots(figsize=(9, 4))
    b = ax.barh(labs[::-1], vals[::-1], color=NARANJA)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g5. Top 5 ciudades de procedencia")
    ax.set_xlabel("Estudiantes"); ax.grid(axis="x", alpha=.3)
    guardar(fig, "g5_top_ciudades.png", "concentracion geografica")

    # g6 semestre promedio por programa
    sp = sem_prog.orderBy(F.desc("estudiantes")).collect()
    labs = [r["programa"] for r in sp]
    vals = [r["semestre_prom"] for r in sp]
    fig, ax = plt.subplots(figsize=(9, 4))
    b = ax.bar(labs, vals, color=GRIS)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g6. Semestre promedio por programa")
    ax.set_ylabel("Semestre promedio"); ax.set_ylim(0, max(vals) * 1.25)
    plt.xticks(rotation=25, ha="right", fontsize=8)
    ax.grid(axis="y", alpha=.3)
    guardar(fig, "g6_semestre_promedio_programa.png", "avance academico por programa")

    # g7 brecha de genero por programa
    bp = brecha.orderBy(F.desc("total")).collect()
    labs = [r["programa"] for r in bp]
    vals = [r["brecha"] for r in bp]
    cols = [ROJO if v > 0 else VERDE for v in vals]
    fig, ax = plt.subplots(figsize=(9, 4))
    b = ax.barh(labs[::-1], vals[::-1], color=cols[::-1])
    ax.bar_label(b, padding=2, fontsize=8)
    ax.axvline(0, color="black", lw=.8)
    ax.set_title("g7. Brecha de género por programa (% hombres - % mujeres)")
    ax.set_xlabel("Brecha porcentual   <-- hombres          mujeres -->")
    ax.grid(axis="x", alpha=.3)
    guardar(fig, "g7_brecha_genero_programa.png", "sesgo de genero por programa")

    emit("")
    emit(f"Total graficos generados: {len([f for f in os.listdir(GRAF_DIR) if f.endswith('.png')])}")

    # ---------------------------------------------------------------- PASO 4
    regla("PASO 4 - EVIDENCIA EN CONSOLA (.show(20, truncate=False))")
    emit("")
    emit("[.show] Top 5 programas:")
    top5.show(20, truncate=False)
    emit("")
    emit("[.show] Distribucion por genero:")
    dist_genero.show(20, truncate=False)
    emit("")
    emit("[.show] Brecha de genero por programa:")
    brecha.show(20, truncate=False)

    regla("PASO 5 - MUESTRA DEL DATASET LIMPIO (primeras 20 filas)")
    muestra = df_l.select("id", "periodo", "institucion", "codigo_programa",
                          "programa", "semestre", "genero", "estrato",
                          "ciudad", "departamento").orderBy(F.col("id").cast("integer"))
    for l in tabla(muestra, 20):
        emit(l)

    fin = datetime.datetime.now()
    regla("METADATOS DE LA EJECUCION")
    emit(f"Inicio       : {inicio:%Y-%m-%d %H:%M:%S}")
    emit(f"Fin          : {fin:%Y-%m-%d %H:%M:%S}")
    emit(f"Duracion     : {(fin - inicio).total_seconds():.1f} segundos")
    emit(f"Spark        : {spark.version}")
    emit(f"Python       : {sys.version.split()[0]}")
    emit(f"Java         : {spark._jvm.java.lang.System.getProperty('java.version')}")
    emit(f"matplotlib   : {matplotlib.__version__} (backend Agg, headless)")
    emit(f"Modo         : local[*]  (batch, sin interfaz grafica)")
    emit(f"Dataset      : {ruta_csv}")
    emit(f"Estudiantes  : {total_final:,}")
    emit(f"Graficos     : {len([f for f in os.listdir(GRAF_DIR) if f.endswith('.png')])} PNG")

    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(LOG) + "\n")

    spark.stop()
    print("")
    print("=" * 78)
    print("  ANALISIS BATCH FINALIZADO")
    print("=" * 78)
    print(f"  Reporte : {REPORT}")
    print(f"  Graficos: {GRAF_DIR}")
    print(f"  Datos   : {total_final:,} estudiantes")
    print("=" * 78)


if __name__ == "__main__":
    main()