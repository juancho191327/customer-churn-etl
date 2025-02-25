import os
import kaggle
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de credenciales de MySQL
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Configuración de Kaggle
KAGGLE_DATASET = "blastchar/telco-customer-churn"
DATASET_FILE = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
DATA_PATH = "data/"
EVIDENCE_PATH = "evidencias/"

def save_evidence(df, step_name):
    """Guarda una muestra de datos y estadísticas en archivos CSV como evidencia."""
    if not os.path.exists(EVIDENCE_PATH):
        os.makedirs(EVIDENCE_PATH)

    df.head(10).to_csv(f"{EVIDENCE_PATH}/{step_name}_muestra.csv", index=False)
    df.describe().to_csv(f"{EVIDENCE_PATH}/{step_name}_estadisticas.csv")
    df.isnull().sum().to_csv(f"{EVIDENCE_PATH}/{step_name}_valores_nulos.csv")

    print(f"📁 Evidencias guardadas en {EVIDENCE_PATH}")

def extract_data():
    """Descarga datos desde Kaggle y los guarda en un DataFrame con validaciones."""
    
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    # Descargar el dataset de Kaggle
    kaggle.api.dataset_download_files(KAGGLE_DATASET, path=DATA_PATH, unzip=True)

    # Cargar datos en un DataFrame
    file_path = os.path.join(DATA_PATH, DATASET_FILE)
    df = pd.read_csv(file_path)

    print("✅ Datos extraídos de Kaggle con éxito.")

    # Registrar extracción
    with open(f"{EVIDENCE_PATH}/log_extraccion.txt", "w") as log_file:
        log_file.write("Datos extraídos correctamente desde Kaggle.\n")
    
    # Validaciones y limpieza
    print("🔎 Validando y limpiando datos...")

    # Valores nulos
    nulls = df.isnull().sum()
    if nulls.any():
        print("⚠️ Se detectaron valores nulos. Guardando evidencia...")
        df[df.isnull().any(axis=1)].to_csv(f"{EVIDENCE_PATH}/valores_nulos.csv", index=False)

    # Duplicados
    duplicados = df[df.duplicated()]
    if not duplicados.empty:
        print("⚠️ Se detectaron valores duplicados. Guardando evidencia...")
        duplicados.to_csv(f"{EVIDENCE_PATH}/duplicados.csv", index=False)

    # Limpieza de TotalCharges
    df['TotalCharges'] = df['TotalCharges'].astype(str).str.strip()  # Quitar espacios
    df['TotalCharges'] = df['TotalCharges'].replace("", None)        # Vacíos -> None
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')  # Convertir a float

    print("✅ Datos limpiados correctamente.")

    # Guardar evidencia después de limpieza
    save_evidence(df, "datos_limpiados")

    return df

def load_data(df):
    """Carga los datos en MySQL con evidencias de inserción."""
    
    # Conectar a MySQL
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # Crear base de datos si no existe
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")

    # Crear tabla si no existe
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_churn_extract (
            customerID VARCHAR(50) PRIMARY KEY,
            gender ENUM('Male', 'Female') NOT NULL,
            SeniorCitizen TINYINT(1) NOT NULL,
            Partner ENUM('Yes', 'No') NOT NULL,
            Dependents ENUM('Yes', 'No') NOT NULL,
            tenure INT NOT NULL,
            PhoneService ENUM('Yes', 'No') NOT NULL,
            MultipleLines VARCHAR(50),
            InternetService ENUM('DSL', 'Fiber optic', 'No') NOT NULL,
            OnlineSecurity ENUM('Yes', 'No', 'No internet service') NOT NULL,
            OnlineBackup ENUM('Yes', 'No', 'No internet service') NOT NULL,
            DeviceProtection ENUM('Yes', 'No', 'No internet service') NOT NULL,
            TechSupport ENUM('Yes', 'No', 'No internet service') NOT NULL,
            StreamingTV ENUM('Yes', 'No', 'No internet service') NOT NULL,
            StreamingMovies ENUM('Yes', 'No', 'No internet service') NOT NULL,
            Contract ENUM('Month-to-month', 'One year', 'Two year') NOT NULL,
            PaperlessBilling ENUM('Yes', 'No') NOT NULL,
            PaymentMethod ENUM('Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)') NOT NULL,
            MonthlyCharges FLOAT,
            TotalCharges FLOAT DEFAULT NULL,
            Churn ENUM('Yes', 'No') NOT NULL
        );
    """)

    print("✅ Base de datos y tabla verificadas.")

    # Insertar datos en MySQL
    print("📥 Cargando datos en MySQL...")
    registros_insertados = 0

    for _, row in df.iterrows():
        sql = """
            INSERT INTO customer_churn_extract VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE customerID=customerID
        """
        values = tuple(row.where(pd.notna(row), None).to_dict().values())  # Manejo de valores NaN correctamente
        cursor.execute(sql, values)
        registros_insertados += 1

    # Confirmar cambios y cerrar conexión
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ {registros_insertados} registros cargados en MySQL.")

    # Registrar en logs
    with open(f"{EVIDENCE_PATH}/log_carga.txt", "w") as log_file:
        log_file.write(f"Se cargaron {registros_insertados} registros en MySQL.\n")

    print("📁 Evidencia de carga guardada en evidencias/log_carga.txt.")

if __name__ == "__main__":
    df = extract_data()
    load_data(df)


