import os
import kaggle
import pandas as pd
import mysql.connector
import random
import string
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

# Generación de datos sintéticos
def generate_customer_id():
    return f"{random.randint(1000, 9999)}-{''.join(random.choices(string.ascii_uppercase, k=5))}"


def generate_synthetic_data(num_samples):
    synthetic_data = []
    
    for _ in range(num_samples):
        sample = {
            "customerID": generate_customer_id(),
            "gender": random.choice(["Male", "Female"]),
            "SeniorCitizen": random.choice([0, 1]),
            "Partner": random.choice(["Yes", "No"]),
            "Dependents": random.choice(["Yes", "No"]),
            "tenure": random.randint(1, 72),
            "PhoneService": random.choice(["Yes", "No"]),
            "MultipleLines": random.choice(["Yes", "No", "No phone service"]),
            "InternetService": random.choice(["DSL", "Fiber optic", "No"]),
            "OnlineSecurity": random.choice(["Yes", "No", "No internet service"]),
            "OnlineBackup": random.choice(["Yes", "No", "No internet service"]),
            "DeviceProtection": random.choice(["Yes", "No", "No internet service"]),
            "TechSupport": random.choice(["Yes", "No", "No internet service"]),
            "StreamingTV": random.choice(["Yes", "No", "No internet service"]),
            "StreamingMovies": random.choice(["Yes", "No", "No internet service"]),
            "Contract": random.choice(["Month-to-month", "One year", "Two year"]),
            "PaperlessBilling": random.choice(["Yes", "No"]),
            "PaymentMethod": random.choice(["Electronic check", "Mailed check", "Bank transfer (automatic)", "Credit card (automatic)"]),
            "MonthlyCharges": round(random.uniform(20.0, 120.0), 2),
            "TotalCharges": round(random.uniform(20.0, 8000.0), 2),
            "Churn": random.choice(["Yes", "No"])
        }
        synthetic_data.append(sample)

    return pd.DataFrame(synthetic_data)

# Guardar evidencias
def save_evidence(df, step_name):
    if not os.path.exists(EVIDENCE_PATH):
        os.makedirs(EVIDENCE_PATH)
    df.head(10).to_csv(f"{EVIDENCE_PATH}/{step_name}_muestra.csv", index=False)
    df.describe().to_csv(f"{EVIDENCE_PATH}/{step_name}_estadisticas.csv")
    print(f"📁 Evidencias guardadas en {EVIDENCE_PATH}")

# Extracción de datos
def extract_data():
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)
    kaggle.api.dataset_download_files(KAGGLE_DATASET, path=DATA_PATH, unzip=True)
    df = pd.read_csv(f"{DATA_PATH}/{DATASET_FILE}")
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    synthetic_df = generate_synthetic_data(5000)
    df = pd.concat([df, synthetic_df], ignore_index=True)
    
    # Eliminar duplicados
    df.drop_duplicates(subset=["customerID"], keep="first", inplace=True)
    
    # Verificar si hay menos de 10000 registros y generar más si es necesario
    while len(df) < 10000:
        remaining = 10000 - len(df)
        additional_df = generate_synthetic_data(remaining)
        df = pd.concat([df, additional_df], ignore_index=True)
        df.drop_duplicates(subset=["customerID"], keep="first", inplace=True)
    
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