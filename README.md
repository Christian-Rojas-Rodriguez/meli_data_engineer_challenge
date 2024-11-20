# meli_data_engineer_challenge

## Desafio

Este es el desafío:
Usando python:
Armar una API (preferentemente) o Script que contenga los siguientes pasos (pueden ser diferentes endpoints o diferentes scripts para cada paso):
Descargar la data:
Descargar 500 resultados de la api de search de meli (podés usar otro criterio de búsqueda) https://api.mercadolibre.com/sites/MLA/search?q=chromecast&limit=50#json
Modelar:
Armar un modelo relacional de al menos 3 tablas que pueda ser creado en bigquery (DDL) con los resultados obtenidos. Mientras que el modelo tenga al menos 3 tablas podes tener la cantidad de campos nested que quieras tener. tomar la decisión de que tablas elegiste en la devolución del challenge vamos a charlar por qué elegiste las que elegiste.
Entregable: DDL con la creación de tablas para ser ejecutadas en BQ
Parsear:
Tomar la data descargada y parsear los files para que puedan ser importados en el modelo que armaste.
Entregable: files en formato json para ser importados en las tablas creadas anteriormente y código utilizado
Looker:
Utilizar el SDK lookml (https://lkml.readthedocs.io/en/latest/ o https://github.com/symphonyrm/lookml-gen) para crear los archivos lookml con una view por cada tabla y un explore que las relacione y pueda ser luego importado en el repositorio de looker (Este sdk sirve para leer archivos lookml y convertirlos en json y viceversa)
Entregable: files en formato lookml con las views y explores creados y el código para crearlos
Se valorará como extra el uso de POO y coverage del código
En caso de utilizar herramientas de GenAI detallar los prompts que fuiste haciendo y como fuiste refinando la búsqueda
Se valorará la entrega en repositorio de github
### Arquitectura de Archivos con Flask

## **1. Estructura del Proyecto**

El proyecto esta organizado en una estructura modular con servicios independientes:

```arduino

meli_data_engineer_challenge/
├── downloader/
│   ├── app.py
│   ├── downloader.py
│   └── tests/
├── modeler/
│   ├── app.py
│   ├── modeler.py
│   └── tests/
├── parser/
│   ├── app.py
│   ├── parser.py
│   └── tests/
├── lookml_generator/
│   ├── app.py
│   ├── lookml_generator.py
│   └── tests/
├── static/
├── templates/
└── .venv/

```

---

## **2. Requisitos Previos**

- Python 3.11 instalado.
- Entorno virtual configurado (`.venv`).
- Dependencias instaladas en cada servicio mediante `pip install -r requirements.txt`.
- Acceso a una cuenta de Google Cloud con BigQuery habilitado.
- Credenciales de Google Cloud configuradas para cada servicio.

---

## **3. Configuración Común para Todos los Servicios**

1. **Configura el Entorno Virtual**:
    
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # Linux/MacOS
    .venv\Scripts\activate     # Windows
    ```
    
2. **Instala Dependencias**:
Desde la raíz de cada servicio (por ejemplo, `downloader/`, `modeler/`):
    
    ```bash
    pip install -r requirements.txt
    ```
    
3. **Configura las Variables de Entorno**:
Crea un archivo `.env` o configura las variables manualmente. Ejemplo:
    
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="ruta_a_tu_archivo.json
    ```
    
    Para Windows (PowerShell):
    
    ```bash
    $env:GOOGLE_APPLICATION_CREDENTIALS="ruta_a_tu_archivo.json"
    ```
    

---

## **4. Ejecución Local de los Servicios**

### **A. Downloader**

### Descripción

- Descarga datos desde la API de MercadoLibre y los guarda en un archivo JSON.

### Ejecución

Desde la carpeta `downloader/`:

```bash
python app.py
```

### Endpoint en Postman

- **URL**: `http://127.0.0.1:5001/download`
- **Método**: `POST`
- **Cuerpo (opcional)**:
    
    ```json
    {
        "query": "chromecast",
        "limit": 50
    }
    ```
    
- **Respuesta Exitosa**:
    
    ```json
    {
        "status": "success",
        "message": "Datos descargados exitosamente."
    }
    ```
    

---

### **B. Modeler**

### Descripción

- Crea tablas en BigQuery basándose en un esquema predefinido.

### Ejecución

Desde la carpeta `modeler/`:

```bash
python app.py
```

### Endpoint en Postman

- **URL**: `http://127.0.0.1:5002/create_tables`
- **Método**: `POST`
- **Cuerpo**: No requiere.
- **Respuesta Exitosa**:
    
    ```json
    {
        "status": "success",
        "message": "Tablas creadas exitosamente."
    }
    ```
    

---

### **C. Parser**

### Descripción

- Procesa los datos descargados y los transforma para cargarlos en BigQuery.

### Ejecución

Desde la carpeta `parser/`:

```bash
python app.py
```

### Endpoint en Postman

- **URL**: `http://127.0.0.1:5003/etl`
- **Método**: `POST`
- **Cuerpo**:
    
    ```json
    {
        "input_file": "downloader/data.json"
    }
    ```
    
- **Respuesta Exitosa**:
    
    ```json
    {
        "status": "success",
        "message": "Datos procesados y cargados exitosamente."
    }
    ```
    

---

### **D. LookML Generator**

### Descripción

- Genera archivos LookML basados en las tablas existentes en BigQuery.

### Ejecución

Desde la carpeta `lookml_generator/`:

```bash
python app.py
```

### Endpoints en Postman

1. **Generar Vistas**
    - **URL**: `http://127.0.0.1:5004/generate_views`
    - **Método**: `POST`
    - **Cuerpo**:
        
        ```json
        {
            "tables": ["Producto", "Vendedor", "Envio"]
        }
        
        ```
        
    - **Respuesta Exitosa**:
        
        ```json
        {
            "status": "success",
            "message": "Views generadas exitosamente."
        }
        
        ```
        
2. **Exportar LookML**
    - **URL**: `http://127.0.0.1:5004/export_lookml`
    - **Método**: `POST`
    - **Cuerpo**:
        
        ```json
        {
            "output_dir": "lookml_generator",
            "explore_name": "producto_explore"
        }
        ```
        
    - **Respuesta Exitosa**:
        
        ```json
        {
            "status": "success",
            "message": "Archivos LookML exportados."
        }
        ```
        

---

## **5. Pruebas Unitarias**

Desde la carpeta de cada servicio, ejecuta:

```bash
pytest --cov=. tests/
```

Para generar un reporte de cobertura:

```bash
coverage run -m pytest tests/
coverage report -m
```

---

## **6. Validaciones Adicionales**

### **BigQuery**

- Asegúrate de que las credenciales de Google Cloud (`GOOGLE_APPLICATION_CREDENTIALS`) están configuradas y tienen permisos suficientes.
- Revisa que las tablas se hayan creado correctamente en el dataset especificado.

### **Postman**

- Importa una colección con los endpoints mencionados.
- Asegúrate de que los cuerpos de las solicitudes estén bien formateados.
