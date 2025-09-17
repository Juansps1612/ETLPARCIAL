# Config/config.py
class Config:
    """
    Configuración de rutas y parámetros para ETL.
    """
    INPUT_PATH = "euro_lineups.csv"   # archivo en raíz
    OUTPUT_PATH = "euro_lineups_clean"  # Spark genera carpeta, no .csv único

    # Configuración de base de datos con JDBC
    DB_URL = "jdbc:sqlite:/workspaces/ETLPARCIAL/eurolineups.db"
    DB_DRIVER = "org.sqlite.JDBC"
    DB_TABLE = "euro_lineups"

