# Guion de exposición de Andina Sport

Diez minutos de presentación y demostración. El docente dispone de cinco minutos adicionales para preguntas. Asignen al menos un tramo a cada integrante. Antes de empezar, verifiquen la conexión de los servicios y abran la tienda en el navegador. El archivo HTML autónomo permite practicar la navegación y leer la evidencia precalculada. Para mostrar el flujo por Kafka, Cassandra, S3 y Neo4j, usen el servidor conectado.

| Tiempo | Apoyo | Qué explicar o demostrar |
|---|---|---|
| 0:00–0:45 | Diapositiva 1 | «Si una persona consulta una colchoneta, ¿qué otros artículos deportivos le podemos mostrar? En Andina combinamos un histórico de valoraciones con señales de navegación de esta tienda». |
| 0:45–1:45 | Diapositiva 2 | Abrir el origen oficial y la categoría Sports_and_Outdoors. Mencionar 500.000 filas leídas, 498.717 limpias y el ASIN padre como identificador del producto. Una reseña favorable alimenta el histórico. |
| 1:45–3:00 | Diapositiva 3 | Seguir la rama de reseñas hasta el modelo. Después seguir un evento del visitante desde el navegador hasta Kafka, las escrituras en Cassandra y S3 y la actualización del grafo. |
| 3:00–4:00 | Diapositiva 4 | Spark calcula pares de artículos que comparten revisores. Neo4j busca hasta 30 revisores con coincidencias. Precalcular el modelo y consultar la vecindad en el momento permite incorporar las señales del visitante. |
| 4:00–5:15 | Escaparate, diapositiva 5 | Abrir una ficha deportiva, por ejemplo la colchoneta Gaiam, añadir una unidad a la bolsa y confirmar el pedido de demostración. Anotar qué ASIN se compró y recordar que los precios son de práctica. |
| 5:15–6:30 | Laboratorio conectado | Consultar la sesión después de la compra. Señalar el tipo, la partición, el offset y el identificador. Abrir el último objeto de S3 y comprobar que corresponde al mismo evento. |
| 6:30–7:30 | Consulta en vivo | Consultar el perfil de prueba en Neo4j. Mostrar un ASIN del ranking y explicar el recorrido por la relación VALORO. Si se usa Neo4j Browser, utilizar la consulta de abajo con el perfil indicado. |
| 7:30–9:00 | Diapositiva 6 | Explicar el corte global del 26 de marzo de 2022. De los 200 perfiles evaluados, Spark tuvo 0 con algún acierto y Neo4j tuvo 2. Los resultados bajos son compatibles con el escaso solapamiento entre historiales. |
| 9:00–10:00 | Diapositiva 7 | La mediana de la consulta en Neo4j fue 6,90 ms y su percentil 95 fue 16,72 ms. El ranking por lote de Spark tardó 3,33 s para la cohorte. Cerrar con el siguiente experimento, una referencia de popularidad y varios cortes temporales. |

## Consulta alternativa en Neo4j Browser

Con el modo conectado y el modelo histórico cargado, abra http://localhost:7474 con las credenciales locales del proyecto. En una celda ejecute lo siguiente.

```
:param usuario => '1526367137417795ebb7aee9'
```

En una celda nueva ejecute la consulta.

```cypher
MATCH (u:RevisorAndino {id: $usuario})-[:VALORO]->(p:ArticuloAndino)<-[:VALORO]-(v:RevisorAndino)
WHERE v <> u
WITH u, v, count(DISTINCT p) AS comunes
ORDER BY comunes DESC, v.id LIMIT 30
MATCH (v)-[:VALORO]->(c:ArticuloAndino)
WHERE NOT EXISTS { MATCH (u)-[:VALORO]->(c) }
RETURN c.asin AS producto, sum(comunes) AS puntos
ORDER BY puntos DESC, producto LIMIT 5
```

Para este perfil, el artículo B0BGFR76CF ocupó el tercer lugar en la corrida medida. También está en sus objetivos futuros. El identificador es el hash del revisor del histórico.

## Preguntas que conviene repartir entre el equipo

- **¿Qué mide Hit@5?** La proporción de perfiles que obtienen al menos un artículo de su lista futura en los cinco primeros resultados.
- **¿De dónde sale el corte?** Spark calcula el cuantil aproximado 0,90 sobre las fechas limpias. Solo las observaciones anteriores construyen la matriz de afinidad.
- **¿Por qué una tabla por consulta?** El perfil se recupera por visitante. Para una sesión se conoce la partición compuesta por visitante y sesión. En ella se leen sus acciones, con el instante y el identificador como claves de orden.
- **¿Por qué hay tan pocos aciertos?** La mayoría de los revisores tiene pocos productos favorables en la proyección. Los pocos vecinos con historial compartido limitan el pronóstico.
- **¿Por qué el laboratorio evalúa más referencias que el escaparate?** La evaluación histórica compara 1.209 artículos. El escaparate deportivo selecciona 20 referencias reales con sus metadatos. El universo y los resultados del experimento histórico se conservan para comparar sus dos modelos.
- **¿Se hizo una compra real?** La persona confirma un pedido de práctica. La tienda registra su acción como un pedido de práctica.
- **¿Cuánto costaría S3?** El laboratorio usa Moto. Para Amazon S3 se requerirían la región, el volumen almacenado, la cantidad de solicitudes y las transferencias para estimar costos.

## Evidencias y participación

Cada integrante identifica en el informe una contribución real y su evidencia, valida su propia parte y conserva sus commits. La autoevaluación y coevaluación se diligencia por separado y se entrega de manera confidencial al docente.
