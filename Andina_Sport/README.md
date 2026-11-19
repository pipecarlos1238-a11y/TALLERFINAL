# Andina Sport

Tienda deportiva del Reto 2 con 20 artículos de fútbol, baloncesto, entrenamiento, running, natación y ciclismo. El catálogo se puede buscar, filtrar y ordenar. Cada ficha muestra una foto, valoración, precio en COP y ASIN de origen. La bolsa permite cambiar cantidades y confirmar pedidos de demostración.

**[Abrir la tienda pública](https://tienda-andina-rutas-ibague.mythicboot2051.chatgpt.site/)**

## Abrir la tienda

- En el enlace público puedes probar el catálogo y las recomendaciones desde un celular o un computador. Cada navegador conserva sus propios favoritos, bolsa e historial.
- Para una presentación sin internet abre **Tienda_Andina.html** con doble clic. Las fotos, fuentes y modelos están incluidos.
- En el pie de la página, **Laboratorio del reto** abre el seguimiento de eventos, siete consultas y la comparación de modelos. **Ver ejecución guardada** reproduce una prueba real con los servicios. Una sesión iniciada con Docker puede además emitir sus propios eventos en vivo.

## Usarla con el taller que ya tienes en VS Code

Del ZIP nuevo, copia **Tienda_Andina.html** y la carpeta **web** a tu carpeta **reto2**, junto a **docker-compose.yml**. Si ya configuraste tu contraseña, conserva el archivo .env existente. Copia también **documentacion** si quieres los materiales de exposición en esa carpeta.

Abre Docker Desktop. En la terminal de PowerShell de VS Code, dentro de **reto2**, ejecuta cada línea.

```powershell
docker compose up -d kafka cassandra neo4j s3
docker compose build app
docker compose run --rm -p 127.0.0.1:8080:8080 app python web/servidor.py
```

Deja esa terminal abierta y entra a [la tienda local](http://localhost:8080/). En [el laboratorio local](http://localhost:8080/#laboratorio) espera el indicador **Servicios conectados**. El primer arranque prepara las tablas y la proyección del grafo. Si un servicio se demoró, puedes pulsar **Consultar servicios**. Para terminar el servidor usa Ctrl+C. Para parar la infraestructura desde la misma carpeta puedes usar `docker compose stop`.

## Ejecutar el ZIP como proyecto independiente

Extrae el ZIP completo y abre la carpeta **Andina_Sport** desde **Archivo → Abrir carpeta** en VS Code. Usa Docker Desktop en modo contenedores Linux. La primera descarga de imágenes requiere internet. Si aún no tienes un archivo .env, copia el modelo con PowerShell.

```powershell
Copy-Item .env.example .env
```

En .env escribe una contraseña propia de al menos ocho caracteres después de `NEO4J_PASSWORD=` y guarda. Ejecuta las tres órdenes de la sección anterior. El puerto 8080 debe estar disponible en tu equipo. Compose crea los contenedores y los volúmenes necesarios.

## Recorrido para exponer

1. Busca **colchoneta** y abre **Colchoneta Premium 5 mm**, de Gaiam. Añade una unidad a la bolsa y confirma el pedido de prueba.
2. Pulsa **Ver mis recomendaciones**. Cambia entre Spark y Neo4j. En una sesión nueva las **Bandas de cadera · 3 uds.**, de BERTER, encabezan las sugerencias de Neo4j. La página indica cuándo completa la selección por popularidad.
3. En **Laboratorio del reto → Mi sesión**, elige Kafka, Cassandra o S3 para inspeccionar el evento. La compra de referencia registró 32 puntos de afinidad para las bandas. El botón **Comparación** muestra el benchmark temporal de los dos modelos.

## Archivos de entrega

| Ubicación | Contenido |
|---|---|
| Tienda_Andina.html | Página completa, con fotos, búsqueda, favoritos, bolsa y recomendaciones |
| web/interfaz | HTML, CSS y JavaScript editables. La carpeta recursos contiene fichas, imágenes y créditos |
| work/amazon/resenas.tsv | Muestra reproducible de 500.000 reseñas válidas, con fecha, identificador público de revisor, ASIN padre, valoración y compra verificada. Archivo tabulado sin encabezado |
| web/modelo | Aristas históricas, cohorte de evaluación, predicciones y afinidades del catálogo deportivo |
| web/evidencia/ejecucion.json | Lecturas confirmadas de la compra deportiva de referencia |
| web/servidor.py | API local para Kafka, Cassandra, Neo4j y S3 |
| documentacion | Informe, presentación PDF y PowerPoint, video narrado, guion, guía rápida y coevaluación individual |
| historial_codigo.bundle | Historial Git del proyecto. Para abrirlo usa `git clone historial_codigo.bundle AndinaEquipo` |

## Datos y reproducción del modelo

Se usaron las primeras 500.000 reseñas válidas de la categoría Sports and Outdoors de Amazon Reviews 2023. La proyección limpia conserva 498.717 filas distintas. El benchmark compara los mismos 200 perfiles antes y después del 26 de marzo de 2022. El universo histórico se fijó con los 1.200 artículos con mayor apoyo previo y las referencias del experimento inicial guardadas en seleccion_experimento.json. El universo completo contiene 1.209 ASIN y 65.160 relaciones positivas. La vitrina de esta edición selecciona 20 de esos artículos, con 45 parejas que comparten al menos un revisor positivo.

La muestra tabulada está incluida en el ZIP. La huella SHA-256 del archivo está en web/modelo/spark.json. Para reconstruir el experimento en una copia del ZIP completo, inicia la infraestructura y ejecuta las siguientes órdenes. Las mediciones de tiempo dependen del equipo.

```powershell
docker compose run --rm spark web/entrenar.py
docker compose run --rm app python web/grafo_amazon.py
docker compose run --rm app python web/compilar_html.py
```

Si cambias la selección por otros artículos del mismo universo, actualiza las fichas y las imágenes de la interfaz y el listado **web/modelo/seleccion.json**. Ejecuta `docker compose run --rm spark web/actualizar_catalogo.py` para recalcular las afinidades de la vitrina y después vuelve a compilar el HTML. Para volver a descargar las 500.000 filas desde la fuente, usa `docker compose run --rm app python web/fuente.py`.

- [Amazon Reviews 2023 de McAuley Lab](https://amazon-reviews-2023.github.io/)
- [Archivo original de reseñas de la categoría](https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Sports_and_Outdoors.jsonl.gz)
- [Metadatos originales e imágenes](https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Sports_and_Outdoors.jsonl.gz)

## Servicios y alcance de la prueba

La URL pública y el archivo HTML funcionan en el navegador de cada visitante. La integración en vivo se activa en el servidor local de Docker. Las reseñas, las valoraciones y las fotos de producto proceden de Amazon. Los precios y los pedidos son académicos. No se solicitan datos de pago ni de envío.

Kafka publica y relee vistas, clics y compras en andina-web-v1. Cassandra permite recuperar el perfil y consultar los eventos por usuario y sesión. S3 conserva un objeto JSON por evento. El servicio Moto del Compose emula S3 en memoria. Spark calcula similitudes de coocurrencia y Neo4j consulta hasta 30 revisores afines. La prueba envió dos veces el mismo UUID de compra y confirmó un único pedido, el objeto archivado y el ranking de Neo4j. Se validó la configuración de Compose y se ejecutaron Apache Spark, Kafka, Cassandra, Neo4j y Moto para verificar el recorrido en local.

Para la entrega de clase, cada integrante debe completar su participación y remitir su coevaluación por el canal confidencial indicado por el docente.
