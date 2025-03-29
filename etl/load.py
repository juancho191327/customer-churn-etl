import mysql.connector
import os
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de credenciales de MySQL
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def load_transformed_data(df):
    """Carga los datos transformados en la tabla MySQL `customer_churn_transformed`."""
    
    # Conectar a MySQL
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    # Crear la nueva tabla con todas las columnas transformadas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_churn_transformed (
            customerID VARCHAR(50) PRIMARY KEY,
            SeniorCitizen ENUM('No', 'Yes') NOT NULL,
            tenure INT NOT NULL,
            MonthlyCharges FLOAT NOT NULL,
            TotalCharges FLOAT NOT NULL,
            Contract_One_year BOOLEAN,
            Contract_Two_year BOOLEAN,
            PaymentMethod_Credit_card BOOLEAN,
            PaymentMethod_Electronic_check BOOLEAN,
            PaymentMethod_Mailed_check BOOLEAN,
            InternetService_Fiber_optic BOOLEAN,
            InternetService_No BOOLEAN,
            MultipleLines_No_phone_service BOOLEAN,
            MultipleLines_Yes BOOLEAN,
            OnlineSecurity_No_internet_service BOOLEAN,
            OnlineSecurity_Yes BOOLEAN,
            OnlineBackup_No_internet_service BOOLEAN,
            OnlineBackup_Yes BOOLEAN,
            DeviceProtection_No_internet_service BOOLEAN,
            DeviceProtection_Yes BOOLEAN,
            TechSupport_No_internet_service BOOLEAN,
            TechSupport_Yes BOOLEAN,
            StreamingTV_No_internet_service BOOLEAN,
            StreamingTV_Yes BOOLEAN,
            StreamingMovies_No_internet_service BOOLEAN,
            StreamingMovies_Yes BOOLEAN,
            gender_Male BOOLEAN,
            Partner_Yes BOOLEAN,
            Dependents_Yes BOOLEAN,
            PhoneService_Yes BOOLEAN,
            PaperlessBilling_Yes BOOLEAN,
            Churn_Yes BOOLEAN,
            tenure_group VARCHAR(10),
            AvgMonthlySpend FLOAT,
            LongTermContract INT,
            isNewCustomer INT,
            MultipleServices INT,
            LowSpender INT
        );
    """)

    print("✅ Tabla `customer_churn_transformed` verificada.")

    # Insertar datos en MySQL
    print("📥 Cargando datos transformados en MySQL...")
    registros_insertados = 0

    for _, row in df.iterrows():
        sql = """
            INSERT INTO customer_churn_transformed VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                SeniorCitizen=VALUES(SeniorCitizen),
                tenure=VALUES(tenure),
                MonthlyCharges=VALUES(MonthlyCharges),
                TotalCharges=VALUES(TotalCharges),
                Contract_One_year=VALUES(Contract_One_year),
                Contract_Two_year=VALUES(Contract_Two_year),
                PaymentMethod_Credit_card=VALUES(PaymentMethod_Credit_card),
                PaymentMethod_Electronic_check=VALUES(PaymentMethod_Electronic_check),
                PaymentMethod_Mailed_check=VALUES(PaymentMethod_Mailed_check),
                InternetService_Fiber_optic=VALUES(InternetService_Fiber_optic),
                InternetService_No=VALUES(InternetService_No),
                MultipleLines_No_phone_service=VALUES(MultipleLines_No_phone_service),
                MultipleLines_Yes=VALUES(MultipleLines_Yes),
                OnlineSecurity_No_internet_service=VALUES(OnlineSecurity_No_internet_service),
                OnlineSecurity_Yes=VALUES(OnlineSecurity_Yes),
                OnlineBackup_No_internet_service=VALUES(OnlineBackup_No_internet_service),
                OnlineBackup_Yes=VALUES(OnlineBackup_Yes),
                DeviceProtection_No_internet_service=VALUES(DeviceProtection_No_internet_service),
                DeviceProtection_Yes=VALUES(DeviceProtection_Yes),
                TechSupport_No_internet_service=VALUES(TechSupport_No_internet_service),
                TechSupport_Yes=VALUES(TechSupport_Yes),
                StreamingTV_No_internet_service=VALUES(StreamingTV_No_internet_service),
                StreamingTV_Yes=VALUES(StreamingTV_Yes),
                StreamingMovies_No_internet_service=VALUES(StreamingMovies_No_internet_service),
                StreamingMovies_Yes=VALUES(StreamingMovies_Yes),
                gender_Male=VALUES(gender_Male),
                Partner_Yes=VALUES(Partner_Yes),
                Dependents_Yes=VALUES(Dependents_Yes),
                PhoneService_Yes=VALUES(PhoneService_Yes),
                PaperlessBilling_Yes=VALUES(PaperlessBilling_Yes),
                Churn_Yes=VALUES(Churn_Yes),
                tenure_group=VALUES(tenure_group),
                AvgMonthlySpend=VALUES(AvgMonthlySpend),
                LongTermContract=VALUES(LongTermContract),
                isNewCustomer=VALUES(isNewCustomer),
                MultipleServices=VALUES(MultipleServices),
                LowSpender=VALUES(LowSpender);
        """
        values = tuple(row.where(pd.notna(row), None).to_dict().values())
        cursor.execute(sql, values)
        registros_insertados += 1

    # Confirmar cambios y cerrar conexión
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ {registros_insertados} registros cargados en `customer_churn_transformed`.")

    # Registrar en logs
    EVIDENCE_PATH = "evidencias/"
    if not os.path.exists(EVIDENCE_PATH):
        os.makedirs(EVIDENCE_PATH)
    
    with open(f"{EVIDENCE_PATH}/log_carga_transformados.txt", "w") as log_file:
        log_file.write(f"Se cargaron {registros_insertados} registros en `customer_churn_transformed`.\n")

    print("📁 Evidencia de carga guardada en `evidencias/log_carga_transformados.txt`.")