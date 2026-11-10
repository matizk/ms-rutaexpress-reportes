# ms-rutaexpress-reportes

> Entrega de código EV2 del 22-10-2026: publicación mediante `release/ev2` → PR a `main`. Ramas del equipo: `matizk` para trabajo personal, `desarrollo` para integración y `main` para la entrega estable. Ver [COLABORACION.md](COLABORACION.md) y la [guía EV2](https://github.com/matizk/ms-rutaexpress-envios/blob/desarrollo/CONTINUAR_EV2_EQUIPO.md). Reportes conserva su código funcional; el cambio respecto de la entrega anterior es documental.

Daniel informó Reportes activo dentro del AWS existente. El caso integrado `RX-1792623980566` ya completó Azure Duoc, aceptación, ticket y tracking; no implica una nueva prueba de todos los KPI. La documentación del [PR #6](https://github.com/matizk/ms-rutaexpress-reportes/pull/6) ya quedó integrada en `desarrollo` mediante revisión manual; este repositorio no tiene CI configurado. [Fuentes y pendientes del equipo](https://github.com/matizk/ms-rutaexpress-envios/blob/desarrollo/CONTINUAR_EV2_EQUIPO.md). El operador Azure no recibe acceso administrativo a Reportes; se mantiene Cognito Admin. La publicación no modifica infraestructura ni sustituye el informe o la presentación.

Microservicio Spring Boot de reportería operacional para RutaExpress.

## EV3 con Kafka

Con el perfil `kafka`, el consumidor recibe eventos de Envíos y actualiza `report_shipments` en una base PostgreSQL propia. La tabla conserva el último estado de cada envío, incluso si Kafka reentrega un evento anterior. Las consultas de KPI y destinos leen esa tabla.

Los brokers se indican en `KAFKA_BOOTSTRAP_SERVERS`. La conexión a PostgreSQL se configura con `REPORT_DB_URL`, `REPORT_DB_USER` y `REPORT_DB_PASSWORD`. El Compose de EV3 está en el repositorio Envíos.

El BFF envía `/api/report/kafka/topics` al microservicio independiente `ms-rutaexpress-kafka-admin`. Reportes sirve los KPI y los destinos. Prometheus consulta `/actuator/prometheus` en la red interna.

## Responsabilidad

Expone consultas de solo lectura. En Entrega 1 obtiene snapshots desde
`ms-rutaexpress-envios` por una URL interna y calcula KPIs sin escribir ni
modificar envíos. El BFF es el único punto que debe exponer estas rutas al
frontend.

```text
Angular -> BFF -> report -> shipments
```

Este modo de consulta HTTP se usa al arrancar sin el perfil `kafka`. En EV3, las mismas rutas leen la proyección local de Reportes.

## Endpoints

| Ruta | Descripción |
|---|---|
| `GET /api/report/kpis?range=last24h` | Totales, estados activos, entregados, cancelados y lead time promedio. |
| `GET /api/report/top-destinations?range=last7d&limit=5` | Destinos con más envíos. |
| `GET /actuator/health` | Salud del servicio. |

Los rangos permitidos son `last24h`, `last7d` y `last30d`. `limit` acepta de 1 a
20. Las métricas consideran la fecha de creación del envío; para envíos en
estado `ENTREGADO`, `fechaActualizacion` representa la hora de término usada por
el lead time.

## Ejecutar localmente

Requiere JDK 21 y que `ms-rutaexpress-envios` esté disponible en el puerto
5000 o en la URL indicada.

```powershell
$env:SHIPMENTS_URL = 'http://localhost:5000'
.\mvnw.cmd spring-boot:run
curl.exe 'http://localhost:8082/api/report/kpis?range=last24h'
```

## Pruebas y Docker

```powershell
.\mvnw.cmd clean verify
docker build -t rutaexpress-report:local .
docker run --rm -p 8082:8082 -e SHIPMENTS_URL=http://host.docker.internal:5000 rutaexpress-report:local
```

Las pruebas cubren cálculo de KPIs, filtrado por rango, lead time, orden de
destinos y validación de rangos. No se incluyen credenciales ni tokens en este
repositorio.
