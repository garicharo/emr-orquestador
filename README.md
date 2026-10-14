# Pipeline EMR, en dos versiones

Reconstrucción del diseño para estudiarlo y compartirlo. No es el repositorio original: no hay ids de cuenta, ni credenciales, ni el código Spark que corrió en el cluster.

La versión 1 es un solo sistema. La versión 2 es la evolución, cuando el cómputo sale del cluster encendido.

## Servicios de la versión 1

```mermaid
flowchart LR
  yaml["YAML de la tabla"] --> cfn["CloudFormation"]
  cfn --> glue["Glue Data Catalog"]
  sqs["SQS: lote de tareas"] --> driver["EMR: driver Java"]
  driver --> dynamo["DynamoDB: estado"]
  driver --> pool["Pool de hilos"]
  pool --> spark["Spark en Scala"]
  spark --> s3["S3"]
  spark --> glue
  driver --> sns["SNS: resultado"]
```

Cada tabla nueva entra por su YAML. El pipeline aplica un stack de CloudFormation y deja el schema en el catálogo de Glue, con los parámetros de las otras cuentas en el deploy, no en el archivo. Cuando llega trabajo, SQS trae el lote. El driver en Java decide el orden. Spark escribe en S3 y publica la tabla. DynamoDB guarda el estado. SNS avisa cómo terminó.

## Flujo de un lote, de inicio a fin

```mermaid
sequenceDiagram
  participant C as CloudFormation
  participant Q as SQS
  participant J as Driver Java
  participant P as Pool
  participant S as Spark Scala
  participant G as Glue y S3
  participant D as DynamoDB
  participant N as SNS
  C->>G: La tabla ya existe en el catálogo
  Q->>J: Lote, puede venir desordenado
  J->>D: RUNNING por cada tarea que arranca
  par Independientes
    J->>P: Tarea sin padre
    P->>S: Escritura corta o de hasta una hora
    S->>G: delta, histórico o partición
  and Con dependencia
    J->>P: El hijo no ocupa hilo hasta que el padre termina
    P->>S: Escribe solo si el padre salió bien
  end
  alt Todo el lote bien
    J->>Q: Borra el mensaje
    J->>N: BATCH_SUCCESS
  else Alguna tarea falla
    J->>N: BATCH_FAILED
    Note over Q: El mensaje se queda. El hijo del que falló no escribe.
  end
```

Una escritura de una hora mantiene el cluster ocupado. El reloj de inactividad no corre mientras el lote sigue vivo. Empieza cuando ya no hay lotes y la cola no trae trabajo.

## Versión 2, la evolución

```mermaid
flowchart LR
  sqs["SQS"] --> java["Java: contrato y dependencias"]
  java --> glueJob["Glue Job"]
  java --> emrS["EMR Serverless"]
  glueJob --> s3["S3 y Glue Data Catalog"]
  emrS --> s3
  java --> dynamo["DynamoDB"]
  java --> sns["SNS"]
```

El contrato de la tabla no cambia. Cambia quién corre Spark. Java sigue leyendo la cola, el estado y las dependencias, y deja de hospedar el clúster. Glue Jobs o EMR Serverless hacen la escritura y se apagan al terminar. Esta versión no es lo que operó el pipeline original.

## Qué hay en el código

`EmrOrchestrator` es la versión 1: un pool, padres antes que hijos aunque el lote llegue al revés, tareas sin padre en paralelo, borrado de la cola solo si el lote termina bien, estado en el catálogo y aviso por el notificador. `TableWriter` es el lugar del Spark. `IdleWatch` es el reloj del cluster.

`CloudNativeOrchestrator` es la versión 2. `ServerlessJobs.start` representa el job remoto. No hay reloj de cluster en ese proceso.

Los nombres de cuenta y los ARN se pasan al deploy. No viven en este repo.

## Correr

```bash
mvn test
```
