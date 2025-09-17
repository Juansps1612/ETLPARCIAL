# Transform/transformer.py
from pyspark.sql import functions as F

class Transformer:
    """
    Clase para limpiar y transformar los datos con PySpark.
    """

    def __init__(self, df):
        self.df = df

    def _safe_cast_to_int_expr(self, col_name):
        """
        Devuelve una expresión Column que convierte de forma segura strings numéricos
        como "1.0", "1", "1,000" a int. Valores no convertibles -> 0.
        """
        s = F.trim(F.col(col_name).cast("string"))             # limpiar espacios
        s = F.regexp_replace(s, r",", "")                      # quitar separadores de miles
        # si está vacío -> 0
        expr = F.when(s == "" , F.lit(0)) \
                .when(s.rlike(r"^-?\d+(\.\d+)?$"), s.cast("double").cast("int")) \
                .otherwise(F.lit(0))
        return F.coalesce(expr, F.lit(0))

    def clean(self):
        df = self.df

        # 1) Eliminar filas sin nombre de jugador (y sin string vacío)
        if "name" in df.columns:
            df = df.filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))

        # 2) Rellenar valores nulos numéricos con 0 (casting seguro)
        num_cols = [
            "jersey_namber",
            "id_match",
            "id_player",
            "year",
            "start_position_x",
            "start_position_y",
            "height",
            "id_club",
            "weight",
            "id_national_team"
        ]
        for col in num_cols:
            if col in df.columns:
                df = df.withColumn(col, self._safe_cast_to_int_expr(col))

        # 3) Rellenar valores nulos de texto con "Unknown"
        text_cols = [
            "country_code",
            "name",
            "name_shirt",
            "position_national",
            "position_field",
            "position_field_detailed",
            "country_birth",
            "birth_date",
            "start"
        ]
        for col in text_cols:
            if col in df.columns:
                trimmed = F.when(F.trim(F.col(col).cast("string")) == "", None).otherwise(F.col(col).cast("string"))
                df = df.withColumn(col, F.coalesce(trimmed, F.lit("Unknown")))

        print("✨ Transformación de datos completada")
        return df
