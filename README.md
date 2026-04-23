# Trabajo Práctico - Coordinación

## Mensajes

Cuando se inicializa el `MessageHandler`, se identifica mediante un atributo `uid`, generado a partir de un `UUID4`. Este identificador es convertido en un string para ser serializado. Aunque para `UUID4` existe la posibilidad de una colisión, esta es ínfima (1 en 2,71*10^18). 

A cada cliente dentro del gateway le corresponde una instancia de `MessageHandler`. Por lo tanto, los otros nodos toman a ese `uid` como el identificador del cliente.

El identificador del `MessageHandler` se agrega al mensaje cuando se llama a las funciones de serialización. Esto es lo que permite que los otros nodos sepan a qué cliente le pertenece el mensaje.

Para un mensaje de datos, el formato es `[fruit, amount, client_id]`. Para un mensaje EOF, es `[client_id]`. Los nodos `sum` y `aggregation` determinan el tipo de mensaje con el largo del arreglo recibido.

## Coordinación de múltiples sum

Los nodos `sum` tienen dos threads: uno para procesar los mensajes de datos y otro para procesar los mensajes EOF.

Este nodo está compuesto por:

```
input_queue: cola de entrada de mensajes de datos del gateway.
data_output_exchanges: arreglo con los exchanges pertenecientes a cada aggregation.
amount_by_client: diccionario que almacena las frutas procesadas de cada cliente, según su ID.
eof_input_queue: cola de entrada de mensajes EOF.
eof_output_queues: arreglo con las colas pertenecientes a cada sum.
```

Cuando uno de los nodos recibe el EOF desde el gateway, lo reenvía a todas las colas `eof_output_queue`, incluida la suya, para que el otro thread lo procese. El otro thread saca el mensaje EOF de `eof_input_queue` y lo procesa llamando a la función `_process_eof()`.

Un nodo determina a qué `aggregation` debe mandarle sus datos mediante la función `_get_aggregation_id()`, en la que se usa un hashing de tipo `md5`. La decisión de usar este tipo de hash es que no importa que no sea seguro de forma criptográfica, ya que lo que interesa es recibir el mismo número para la misma entrada. De esta forma, al calcular el resto de la divisón entre este número y la cantidad de `aggregators` n, nos dará un número entre 0 y n-1.

## Coordinación de múltiples aggregation

Este nodo está compuesto por:

```
input_exchange: exchange de entrada de datos/EOF de los nodos sum.
output_queue: cola de salida de datos hacia el nodo join.
fruit_top_by_client: diccionario que almacena los datos procesados de un cliente, por su ID.
eof_counter_by_client: diccionario que tiene un contador de mensajes recibidos con el ID del cliente.
```

El `aggregation` no envía los datos a `join` hasta recibir el EOF de todos los `sum` pertenecientes a ese cliente. Almacena el número de EOF recibidos en `eof_counter_by_client[client_id]`, pero sí procesa los datos enviados de un `sum`, almacenándolos en `fruit_top_by_client[client_id]`.

## Coordinación de join

Este nodo está compuesto por:

```
input_queue: cola de entrada de datos desde los nodos aggregation.
output_queue: cola de salida de datos hacia el gateway.
done_aggregations_by_client: diccionario compuesto por un contador por ID del cliente.
tops_by_client: diccionario que almacena los tops parciales recibidos de los aggregation por ID del cliente.
```

El `join` no procesa y envía los datos al gateway hasta recibir los datos de todos los `aggregation` de un cliente. Esto lo hace comparando la cantidad de tops recibidos de los clientes contra la cantidad de nodos `aggregation`. Cuando recibe un mensaje con los tops parciales, lo guarda en `tops_by_client[client_id]`.

## Escalabilidad

Clientes: el uso de `client_id` en el `MessageHandler` permite el procesamiento de múltiples clientes en paralelo al identificar a cuál cliente le corresponde un mensaje.

Sum: distribuyen el procesamiento de datos en paralelo. Como están ligados a una única `input_queue`, los datos de n clientes se distribuyen entre todos los nodos.

Aggregation: al hashear el tipo de fruta y calcular el resto de la división con la cantidad de `aggregation`, se distribuye el trabajo entre todos, en comparación a hashear el ID del cliente, que enviaría todo a uno solo.

## Graceful shutdown

Al recibir la señal SIGTERM, cada nodo deja de consumir del exchange/queue del que está consumiendo mediante `stop_consuming()`, lo que hace que el flujo del proceso salga de la función bloqueante `start_consuming()`. Finalmente, se hace `close()` de todos los queue/exchanges utilizados, para cerrar ambos extremos.

En el caso de los nodos `sum`, al utilizar un thread para procesar los mensajes EOF, al catchear SIGTERM desde el nodo principal es necesario usar `add_callback_threadsafe()` para que el otro thread deje de consumir, ya que Pika no es threadsafe.

## NACK

Ante una excepción al procesar un mensaje, se imprime por log el error y se llama a `nack()`. Para el objetivo de este TP los mensajes no se reencolan.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.