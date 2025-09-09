# Config/config.py
class Config:
    """
    Configuración de rutas y parámetros para ETL.
    """
    INPUT_PATH = "WorldCupPlayers.csv"   # archivo en raíz
    OUTPUT_PATH = "WorldCupPlayers_clean"  # Spark genera carpeta, no .csv único

    # Configuración de base de datos con JDBC
    DB_URL = "jdbc:sqlite:/workspaces/ETLPARCIAL/WorldCupPlayers.db"
    DB_DRIVER = "org.sqlite.JDBC"
    DB_TABLE = "world_cup_players"

