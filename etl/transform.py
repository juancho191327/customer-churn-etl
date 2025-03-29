import pandas as pd

def clean_total_charges(df):
    """Convierte TotalCharges a numérico y elimina valores nulos."""
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
    df.dropna(subset=["TotalCharges"], inplace=True)
    return df

def encode_categorical(df):
    """Aplica one-hot encoding a variables categóricas."""
    categorical_cols = ["Contract", "PaymentMethod", "InternetService", "MultipleLines",
                        "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
                        "StreamingTV", "StreamingMovies", "gender", "Partner", "Dependents", "PhoneService", "PaperlessBilling", "Churn"]
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
    return df

def categorize_senior_citizen(df):
    """Convierte SeniorCitizen en una variable categórica."""
    df["SeniorCitizen"] = df["SeniorCitizen"].map({0: "No", 1: "Yes"})
    return df

def create_tenure_groups(df):
    """Crea una nueva columna para agrupar el tenure en rangos."""
    max_tenure = df["tenure"].max() + 1  # Asegurar que el último bin sea único
    bins = [0, 12, 24, 48, 72, max_tenure]
    labels = ["0-12", "13-24", "25-48", "49-72", "73+"]
    df["tenure_group"] = pd.cut(df["tenure"], bins=bins, labels=labels, include_lowest=True)
    return df

def calculate_avg_monthly_spend(df):
    """Crea la columna AvgMonthlySpend para analizar patrones de gasto."""
    df["AvgMonthlySpend"] = df["TotalCharges"] / df["tenure"]
    df["AvgMonthlySpend"] = df["AvgMonthlySpend"].fillna(df["MonthlyCharges"])  # Evitar divisiones por 0
    return df

def feature_engineering(df):
    """Crea nuevas variables de análisis."""
    df["LongTermContract"] = df[["Contract_One year", "Contract_Two year"]].sum(axis=1)
    df["isNewCustomer"] = (df["tenure"] == 0).astype(int)
    cols_to_check = ["PhoneService", "InternetService_Fiber optic", "StreamingTV", "StreamingMovies"]
    existing_cols = [col for col in cols_to_check if col in df.columns]
    df["MultipleServices"] = df[existing_cols].eq(1).sum(axis=1) if existing_cols else 0
    df["LowSpender"] = (df["AvgMonthlySpend"] < df["AvgMonthlySpend"].median()).astype(int)
    return df

def transform_data(df):
    """Ejecuta todas las transformaciones en orden."""
    df = clean_total_charges(df)
    df = encode_categorical(df)
    df = categorize_senior_citizen(df)
    df = create_tenure_groups(df)
    df = calculate_avg_monthly_spend(df)
    df = feature_engineering(df)
    return df