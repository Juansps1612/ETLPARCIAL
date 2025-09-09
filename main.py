from Config.config import Config
from Extract.extractor import Extractor
from Transform.transformer import Transformer
from pyspark.sql import SparkSession

def main():
    print("🚀 Iniciando proceso ETL...")

    # Crear sesión Spark con el conector JDBC
    spark = SparkSession.builder \
        .appName("ETL World Cup") \
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

    spark.stop()
    print("✅ Proceso ETL completado")

if __name__ == "__main__":
    main()
