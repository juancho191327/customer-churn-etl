from etl.extract import extract_data
#from etl.transform import transform_data
#from etl.load import load_data
from etl.extract import extract_data, load_data
import os

EVIDENCE_PATH = "evidencias/"

def main():
    """Main function to run the ETL pipeline with evidence generation."""
    
    if not os.path.exists(EVIDENCE_PATH):
        os.makedirs(EVIDENCE_PATH)

    try:
        print("\n🚀 Iniciando proceso ETL...")
        
        # 🛠 EXTRACCIÓN
        print("\n📥 Extrayendo datos desde Kaggle...")
        data = extract_data()
        print("✅ Extracción completada.")

        # Guardar muestra de datos extraídos como evidencia
        data.head(10).to_csv(f"{EVIDENCE_PATH}/extraccion_muestra.csv", index=False)
        print(f"📁 Evidencia de extracción guardada en {EVIDENCE_PATH}.")

        # 🛠 CARGA
        print("\n📤 Cargando datos en MySQL...")
        load_data(data)
        print("✅ Carga en base de datos completada.")

        print("\n🎯 ETL finalizado exitosamente. 🚀")

    except Exception as e:
        error_msg = f"❌ Error en el proceso ETL: {str(e)}"
        print(error_msg)
        with open(f"{EVIDENCE_PATH}/log_error.txt", "w") as log_file:
            log_file.write(error_msg)

if __name__ == "__main__":
    main()

    
