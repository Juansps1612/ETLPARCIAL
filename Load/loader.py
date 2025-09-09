# Load/loader.py
from Config.config import Config

class Loader:
    """
    Clase para cargar datos en HDFS/CSV/Parquet con PySpark.
    """

    def __init__(self, df):
        self.df = df

    def to_csv(self, output_path=Config.OUTPUT_PATH):
        try:
            (
                self.df.write
                .mode("overwrite")
                .option("header", True)
                .csv(output_path)
            )
            print(f"💾 Datos guardados en CSV: {output_path}")
        except Exception as e:
            print(f"❌ Error al guardar CSV: {e}")

    def to_parquet(self, output_path=Config.OUTPUT_PATH):
        try:
            (
                self.df.write
                .mode("overwrite")
                .parquet(output_path)
            )
            print(f"💾 Datos guardados en Parquet: {output_path}")
        except Exception as e:
            print(f"❌ Error al guardar Parquet: {e}")
