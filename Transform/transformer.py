# Transform/transformer.py
from pyspark.sql import functions as F

class Transformer:
    """
    Clase para limpiar y transformar los datos con PySpark.
    """

    def __init__(self, df):
        self.df = df

    def clean(self):
        df = self.df

        # Eliminar filas sin nombre de jugador
        if "Player Name" in df.columns:
            df = df.filter(F.col("Player Name").isNotNull())

        # Rellenar valores nulos numéricos con 0
        num_cols = ["RoundID", "MatchID", "Shirt Number"]
        for col in num_cols:
            if col in df.columns:
                df = df.withColumn(col, F.coalesce(F.col(col).cast("int"), F.lit(0)))

        # Rellenar valores nulos de texto con "Unknown"
        text_cols = ["Team Initials", "Coach Name", "Player Name", "Position", "Event"]
        for col in text_cols:
            if col in df.columns:
                df = df.withColumn(col, F.coalesce(F.col(col).cast("string"), F.lit("Unknown")))

        # Convertir "Line-up" a booleano
        if "Line-up" in df.columns:
            df = df.withColumn(
                "Line-up",
                F.when(F.col("Line-up").isin("1", "yes", "true"), True)
                 .when(F.col("Line-up").isin("0", "no", "false"), False)
                 .otherwise(False)
            )

        print("✨ Transformación de datos completada")
        return df
