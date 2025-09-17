from Config.config import Config
from Extract.extractor import Extractor
from Transform.transformer import Transformer
from pyspark.sql import SparkSession
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def main():
    print("🚀 Iniciando proceso ETL...")

    # Crear sesión Spark con el conector JDBC
    spark = SparkSession.builder \
        .appName("ETL Euro") \
        .config("spark.jars", "/workspaces/ETLPARCIAL/sqlite-jdbc-3.36.0.3.jar") \
        .getOrCreate()

    # 1. Extraer
    extractor = Extractor(Config.INPUT_PATH, spark)
    df = extractor.extract()
    if df is None or df.count() == 0:
        print("❌ No se pudieron extraer datos. ETL cancelado.")
        return

    # 2. Transformar
    transformer = Transformer(df)
    df_clean = transformer.clean()

    # 3. Cargar con Spark
    # Guardar CSV (Spark siempre crea carpeta con varios part-files)
    df_clean.write.mode("overwrite").option("header", True).csv(Config.OUTPUT_PATH)
    print(f"💾 Datos guardados en CSV: {Config.OUTPUT_PATH}")

    # Guardar en base de datos SQLite con JDBC
    (
        df_clean.write
        .format("jdbc")
        .option("url", Config.DB_URL)
        .option("driver", Config.DB_DRIVER)
        .option("dbtable", Config.DB_TABLE)
        .mode("overwrite")
        .save()
    )
    print(f"🗄️ Datos cargados en la base de datos: {Config.DB_TABLE}")

    # 4. Visualización con Seaborn (usamos Pandas para convertir Spark → Pandas)
    pdf = df_clean.toPandas()

    print("📊 Generando gráficos con seaborn...")

    # --- Gráfico 1: Distribución de jugadores por país ---
    plt.figure(figsize=(12,6))
    sns.countplot(data=pdf, x="country_code", order=pdf["country_code"].value_counts().index)
    plt.title("Distribución de jugadores por país")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig("grafico_jugadores_pais.png")
    plt.close()

    # --- Gráfico 2: Altura promedio por posición ---
    if "position_field" in pdf.columns and "height" in pdf.columns:
        plt.figure(figsize=(10,6))
        sns.barplot(data=pdf, x="position_field", y="height", errorbar=None)
        plt.title("Altura promedio por posición en el campo")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("grafico_altura_posicion.png")
        plt.close()

    # --- Gráfico 3: Distribución del peso ---
    if "weight" in pdf.columns:
        plt.figure(figsize=(10,6))
        sns.histplot(data=pdf[pdf["weight"] > 0], x="weight", bins=20, kde=True)
        plt.title("Distribución del peso de los jugadores")
        plt.tight_layout()
        plt.savefig("grafico_peso.png")
        plt.close()

    print("✅ Proceso ETL completado con visualizaciones generadas")

    spark.stop()

if __name__ == "__main__":
    main()

