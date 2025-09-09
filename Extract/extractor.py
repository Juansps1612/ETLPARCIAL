# Extract/extractor.py
from pyspark.sql import SparkSession

class Extractor:
    """
    Clase para extraer datos con Spark.
    """
    def __init__(self, file_path, spark):
        self.file_path = file_path
        self.spark = spark

    def extract(self):
        try:
            df = self.spark.read.option("header", True).csv(self.file_path)
            print(f"📥 Datos extraídos: {df.count()} filas, {len(df.columns)} columnas")
            return df
        except Exception as e:
            print(f"❌ Error al extraer datos: {e}")
            return None
