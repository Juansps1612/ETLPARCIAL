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
            # inferSchema = True ayuda a que Spark detecte tipos numéricos como double
            df = (
                self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("mode", "PERMISSIVE")
                .csv(self.file_path)
            )
            print(f"📥 Datos extraídos: {df.count()} filas, {len(df.columns)} columnas")
            # Si quieres ver el schema para debugging:
            # df.printSchema()
            return df
        except Exception as e:
            print(f"❌ Error al extraer datos: {e}")
            return None

