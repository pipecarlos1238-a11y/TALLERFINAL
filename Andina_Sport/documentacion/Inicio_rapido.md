# Abrir Andina Sport en VS Code

La tienda y el laboratorio están separados. El escaparate permite explorar el catálogo y confirmar una compra académica. El laboratorio se encuentra en la ruta `#laboratorio`. Los materiales de exposición están en la carpeta `documentacion`.

## Explorar el HTML autónomo

Extrae el ZIP y abre el HTML principal con Chrome o Edge. Prueba la búsqueda, los filtros, los favoritos y la bolsa. Las acciones se guardan en el navegador. Es posible revisar las mediciones históricas precalculadas en el laboratorio.

## Preparar la demostración conectada

1. Abre Docker Desktop. En VS Code selecciona **Archivo → Abrir carpeta** y elige la carpeta donde se encuentran `docker-compose.yml` y la carpeta `web`.
2. Abre **Terminal → Nuevo terminal**. Para un proyecto nuevo en Windows, ejecuta `Copy-Item .env.example .env`. Si ya configuraste el entorno en esta misma carpeta, conserva tu archivo `.env`.
3. Escribe en `.env` tu contraseña local de Neo4j, con al menos ocho caracteres, en la variable `NEO4J_PASSWORD`. Guarda el archivo.
4. En la terminal de PowerShell ejecuta cada comando y espera a que termine.

```powershell
docker compose up -d kafka cassandra neo4j s3
docker compose build app
docker compose run --rm -p 127.0.0.1:8080:8080 app python web/servidor.py
```

5. Deja la terminal abierta. En el navegador abre [el escaparate local](http://localhost:8080/). En [el laboratorio local](http://localhost:8080/#laboratorio) comprueba el estado de los servicios. La primera carga prepara las relaciones de afinidad.
6. Abre la ficha de **Colchoneta Premium 5 mm**, de Gaiam, añádela a la bolsa y confirma el pedido de práctica. En el laboratorio consulta esa sesión, localiza su evento de compra y lee la partición, el offset y la clave del objeto. El ranking de productos usa las señales registradas. Las bandas de cadera BERTER comparten cuatro revisores positivos con esta colchoneta en el histórico.

El archivo README de la raíz documenta los requisitos, el orden completo de arranque y la reproducción del experimento. La URL `localhost` abre los servicios que se están ejecutando en el equipo del estudiante.

## Revisar la evidencia Git

Dentro de la carpeta que contiene el archivo `.bundle` de la entrega, estas órdenes de PowerShell crean una copia del repositorio y muestran sus confirmaciones.

```powershell
$historial = Get-ChildItem -File -Filter *.bundle | Select-Object -First 1
git clone $historial.FullName historial-andina-sport
git -C historial-andina-sport log --oneline
```

Para acreditar su aporte, cada integrante registra en el informe el identificador de su propia confirmación o la evidencia del trabajo que realizó. Entregue la coevaluación individual al docente por el canal privado del curso.

## Archivos originales de la muestra

McAuley Lab publica la colección Amazon Reviews 2023. Las valoraciones, fechas e indicadores de verificación de esta entrega proceden de las reseñas de Sports_and_Outdoors. Los nombres identificables de artículos y las direcciones de sus imágenes proceden de los metadatos de la misma categoría.

- [Página oficial de Amazon Reviews 2023](https://amazon-reviews-2023.github.io/)
- [Descargar el archivo original de reseñas](https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Sports_and_Outdoors.jsonl.gz)
- [Descargar el archivo original de metadatos](https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Sports_and_Outdoors.jsonl.gz)
