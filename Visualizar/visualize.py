# visualize.py
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

def main():
    # Leer el CSV limpio generado por el ETL
    df = pd.read_csv("euro_lineups_clean/part-00000*.csv")  # Spark genera varios part-files

    print(f"📊 Dataset cargado: {df.shape[0]} filas, {df.shape[1]} columnas")

    # === 1. Distribución de 'year' ===
    plt.figure(figsize=(8, 5))
    sns.histplot(df["year"], bins=20, kde=True)
    plt.title("Distribución de 'year'")
    plt.xlabel("Año")
    plt.ylabel("Frecuencia")
    plt.show()

    # === 2. Altura vs Peso ===
    plt.figure(figsize=(8, 5))
    sns.scatterplot(data=df, x="height", y="weight", hue="position_field")
    plt.title("Altura vs Peso de jugadores")
    plt.xlabel("Altura (cm)")
    plt.ylabel("Peso (kg)")
    plt.show()

    # === 3. Conteo de jugadores por posición ===
    plt.figure(figsize=(10, 5))
    sns.countplot(data=df, x="position_field")
    plt.title("Distribución de jugadores por posición")
    plt.xlabel("Posición")
    plt.ylabel("Cantidad")
    plt.xticks(rotation=45)
    plt.show()

if __name__ == "__main__":
    main()
