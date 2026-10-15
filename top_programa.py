# -*- coding: utf-8 -*-
"""PROGRAMA MAS SELECCIONADO - Escuela Nacional del Deporte
Pegar en la maquina virtual BIGDATA2 (Ubuntu Server)."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pyspark.sql import SparkSession, functions as F

CSV = ("/home/vboxuser/Descargas/ESTUDIANTES_MATRICULADOS_EN_LA_ESCUELA_NACIONAL_"
       "DEL_DEPORTE_20261014.csv")

spark = (SparkSession.builder.appName("TopPrograma").master("local[*]")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = (spark.read.option("header", "true").option("delimiter", ",")
        .option("encoding", "UTF-8").option("inferSchema", "true").csv(CSV))

df = (df.withColumnRenamed("PROGRAMA", "programa")
        .withColumnRenamed("GÉNERO", "genero")
        .withColumn("programa",
                    F.trim(F.expr("translate(programa,"
                                  "'áéíóúüñÁÉÍÓÚÜÑ','aeiouunAEIOUUN')"))))

top = (df.filter(F.col("programa").isNotNull())
         .groupBy("programa").count().orderBy(F.desc("count")).limit(5))

total = df.count()
filas = top.collect()
primero = filas[0]

print()
print("=" * 62)
print("  PROGRAMA ACADEMICO MAS SELECCIONADO")
print("=" * 62)
print()
print(f"  {primero['programa']}")
print(f"  {primero['count']:,} estudiantes"
      f"  ({primero['count'] * 100.0 / total:.2f}% del total)")
print()
print("  Top 5:")
top.show(5, truncate=False)
print(f"  Universo total: {total:,} estudiantes")
print("=" * 62)

spark.stop()