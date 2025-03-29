# Telco Customer Churn - ETL Project

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) para analizar la tasa de abandono de clientes (churn) en un proveedor de telecomunicaciones. Se descarga un dataset desde Kaggle, se realiza una limpieza y transformación de los datos, y finalmente se carga en una base de datos MySQL.

## 🚀 Requisitos

Antes de ejecutar el proyecto, asegúrate de tener instalados los siguientes requisitos:

- Python 3.12
- Poetry (para la gestión de dependencias)
- MySQL Server
- Una cuenta en Kaggle

## 📂 Instalación y Configuración

### 1️⃣ Clonar el repositorio
```bash
    git clone https://github.com/juancho191327/customer-churn-etl.git
    cd customer-churn-etl
```

### 2️⃣ Configurar un entorno virtual con Poetry
```bash
    poetry install
```

### 3️⃣ Configurar credenciales de Kaggle

Para descargar el dataset, es necesario obtener las credenciales de Kaggle:

1. Accede a [Kaggle](https://www.kaggle.com/) y dirígete a *Account*.
2. Desplázate hasta la sección *API* y haz clic en *Create New API Token*.
3. Se descargará un archivo `kaggle.json`. Mueve este archivo a la carpeta `~/.kaggle/` (en Windows, `C:\Users\tu-usuario\.kaggle\`).

Ejemplo de comando en Linux/Mac:
```bash
    mkdir -p ~/.kaggle
    mv /ruta/al/archivo/kaggle.json ~/.kaggle/
    chmod 600 ~/.kaggle/kaggle.json  # Asegurar permisos adecuados
```

### 4️⃣ Configurar credenciales de MySQL

Crea un archivo `.env` en la raíz del proyecto con la siguiente estructura:
```ini
DB_HOST=localhost
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_NAME=customer_churn
```

## 🛠️ Ejecución del ETL

Para ejecutar el pipeline ETL, simplemente corre el siguiente comando:
```bash
    poetry run python main.py
```

## 📊 Descripción del Pipeline ETL

1️⃣ **Extracción**: Se descarga el dataset `Telco Customer Churn` desde Kaggle y se carga en un DataFrame de Pandas.
2️⃣ **Transformación**: Se limpian los datos, convirtiendo valores vacíos en `NaN` y ajustando los tipos de datos.
3️⃣ **Carga**: Los datos transformados se insertan en una base de datos MySQL en la tabla `customer_churn_extract`.

## 📌 Notas Adicionales
- Asegúrate de que MySQL esté corriendo antes de ejecutar el pipeline.
- Si necesitas limpiar la base de datos y reiniciar la carga, puedes eliminar la tabla con:
```sql
DROP TABLE customer_churn_extract;
```

## 📞 Contacto
Si tienes preguntas o mejoras, no dudes en contribuir o abrir un issue en el repositorio. 🚀

