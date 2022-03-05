# Analisis de datos (ETL) en Databricks (Pyspark)

Este dataset emula una pequeña base de datos de una compañía que vende autopartes, la cual utiliza para llevar el registro de sus clientes, productos y pedidos. Consiste en seis tablas: productos, detalle_productos, detalle_categoria, detalle_cliente, detalle_sucursal y ordenes

Retos en la tranformación de datos:

1.  Quitar acentos y salto de linea para todas las tablas

## Contenido
| Sección                                                                             |        Funciones |
|:------------------------------------------------------------------------------------|:--------------------|
|[1. Creando dataframe a partir de archivos CSV](#1-Creando-dataframe-a-partir-de-archivos-CSV)||
|[2. Tranformaciones en tabla de cliente](#2-Tranformaciones-en-tabla-de-cliente)|TRANSLATE, REGEX_REPLACE, REGEX_EXTRACT, TRIM|
|[3. Tranformaciones en tabla sucursales](#3-Tranformaciones-en-tabla-sucursales)|REGEX_EXTRACT|
|[4. Tranformaciones en la tabla de productos](#4-Tranformaciones-en-la-tabla-de-productos)|TRANSLATE|
|[5. Tabla de hechos](#5-Tabla-de-hechos)|GROUPBY, SUM, ORDERBY , COUNTDISTINCT, FILTER, REGEXP_EXTRACT, FILTER, COL,MULTI TABLE JOINS|

## 1. Creando dataframe a partir de archivos CSV

Las tablas de encuentran en achivos CSV, hacemos una carga de los archivos a Databricks para poder transformarlos. 

```Pyspark
# File location and type
file_clients = "/FileStore/shared_uploads/host@gmail.com/detalle_cliente.csv"
file_sucursal = "/FileStore/shared_uploads/host@gmail.com/detalle_sucursal.csv"
file_products = "/FileStore/shared_uploads/host@gmail.com/productos.csv"
file_details_products = "/FileStore/shared_uploads/host@gmail.com/detalle_productos.csv"
file_category_products = "/FileStore/shared_uploads/host@gmail.com/detalle_categoria.csv"
file_ordenes = "/FileStore/shared_uploads/host@gmail.com/ordenes.csv"

file_type = "csv"

# File options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "|"

# The applied options are for CSV files. For other file types, these will be ignored.
raw_clientes = spark.read.format(file_type).option("inferSchema", infer_schema) \
  .option("header", first_row_is_header).option("multiline", "true").option("sep", delimiter).load(file_clients)

raw_sucursales = spark.read.format(file_type).option("inferSchema", infer_schema) \
  .option("header", first_row_is_header).option("multiline", "true").option("sep", delimiter).load(file_sucursal)

raw_products = spark.read.format(file_type).option("inferSchema", infer_schema) \
  .option("header", first_row_is_header).option("multiline", "true").option("sep", delimiter).load(file_products)

raw_details_products = spark.read.format(file_type).option("inferSchema", infer_schema) \
  .option("header", first_row_is_header).option("multiline", "true").option("sep", delimiter).load(file_details_products)

raw_category_products = spark.read.format(file_type).option("inferSchema", infer_schema) \
  .option("header", first_row_is_header).option("multiline", "true").option("sep", delimiter).load(file_category_products)
  
raw_ordenes = spark.read.format(file_type).option("inferSchema", infer_schema) \
  .option("header", first_row_is_header).option("multiline", "true").option("sep", delimiter).load(file_ordenes)

```

## 2. Tranformaciones en tabla de cliente
#### 2.1 Quitar acento y salto de linea
```Pyspark
#quitando acento y salto de linea en tabla de clientes columnas direccion|clientes
df_clientes = df_clientes.withColumn('direccion', translate(col('direccion'), 'áéíóú', 'aeiou')) \
    .withColumn('cliente', translate(col('cliente'), 'áéíóú', 'aeiou')) \
    .withColumn('direccion', regexp_replace('direccion',r'(\n)',''))
```
ANTES: 
```Pyspark
+----------+-------------------------------+-----------------------------------------------------------------------------------------+
|cliente_id|cliente                        |direccion                                                                                |
+----------+-------------------------------+-----------------------------------------------------------------------------------------+
|C-92AC    |Zeferino Hermelinda Cardenas   |Ampliación Cortés 072 Edif. 362 , Depto. 032
Nueva Belarús, DGO 36879                    |
|C-61Rz    |Judith Santiago Trejo          |Eje vial República Centroafricana 816 Interior 150
San Gustavo los altos, SIN 75788-3053 |
+----------+-------------------------------+-----------------------------------------------------------------------------------------+
```
DESPUES:
```Pyspark
+----------+-------------------------------+----------------------------------------------------------------------------------------+
|cliente_id|cliente                        |direccion                                                                               |
+----------+-------------------------------+----------------------------------------------------------------------------------------+
|C-95aQ    |Isaac Berta Espinosa Bustamante|Prolongacion Norte Guillen 815 833San Vicente de la Montaña, TAMPS 44107                |
|C-98oG    |Genaro Noelia Olivas Quiroz    |Avenida Norte Lozano 005 Edif. 213 , Depto. 580San Pascual de la Montaña, MEX 11015-7667|
|C-92AC    |Zeferino Hermelinda Cardenas   |Ampliacion Cortes 072 Edif. 362 , Depto. 032Nueva Belarus, DGO 36879                    |
|C-61Rz    |Judith Santiago Trejo          |Eje vial Republica Centroafricana 816 Interior 150San Gustavo los altos, SIN 75788-3053 |
+----------+-------------------------------+----------------------------------------------------------------------------------------+
```

#### 2.2 De la columna direccion separar el estado y el código postal
```Pyspark
df_end_cliente = df_clientes.withColumn('estado', regexp_extract(col('direccion'), r'([A-Z]{1}[\.].[A-Z]{3,}|[A-Z]{3,})',1)) \
    .withColumn('codigo_postal', regexp_extract(col('direccion'), r'([0-9]{4,}.*)',1)) \
    .withColumn('cliente', trim('cliente'))
```
Resultado:
```Pyspark
+--------------------------------------------------------------------------------------------+------+-------------+
|direccion                                                                                   |estado|codigo_postal|
+--------------------------------------------------------------------------------------------+------+-------------+
|Pasaje Sur Hinojosa 546 Interior 968Vieja Myanmar, OAX 06497                                |OAX   |06497        |
|Prolongacion Norte Guillen 815 833San Vicente de la Montaña, TAMPS 44107                    |TAMPS |44107        |
|Avenida Norte Lozano 005 Edif. 213 , Depto. 580San Pascual de la Montaña, MEX 11015-7667    |MEX   |11015-7667   |
|Retorno Veracruz de Ignacio de la Llave 424 Interior 386Nueva Tailandia, BCS 49534          |BCS   |49534        |
|Calle Fernandez 479 Interior 434Nueva Etiopia, Q. ROO 93717-4366                            |Q. ROO|93717-4366   |
|Boulevard Norte Centeno 767 Edif. 847 , Depto. 365Vieja Luxemburgo, DGO 53132-9607          |DGO   |53132-9607   |
+--------------------------------------------------------------------------------------------+------+-------------+
```
[Back to Top](#Contenido)

## 3. Tranformaciones en tabla sucursales

De la columna sucursal obtén el estado que corresponde a la cadena de 3 o más letras previas al “-”

```Pyspark
df_end_sucursal = raw_sucursales.withColumn('estado', regexp_extract(col('sucursal'), r'(^[A-Z]{3})',1))
```
Resultado:
```Pyspark
+-----------+--------+------+
|sucursal_id|sucursal|estado|
+-----------+--------+------+
|     C-72pY| AGS-SVu|   AGS|
|     C-41tT| SON-aHW|   SON|
|     C-16ZQ| COL-ojJ|   COL|
+-----------+--------+------+
```
[Back to Top](#Contenido)

## 4. Tranformaciones en la tabla de productos

```Pyspark
#quitando acento y salto de linea tabla de productos en columna nombre
df_clean_products = raw_products.withColumn('nombre', translate(col('nombre'), 'áéíóú', 'aeiou'))
```
ANTES:
```Pyspark
+------+--------------------------------+-------+-------+------------+-----+
|sku   |nombre                          |escala |precio |categoria_id|stock|
+------+--------------------------------+-------+-------+------------+-----+
|G660F4|Inyector de combustible         |1 - 25 |4864.73|p-92w       |5528 |
|L647n5|Medidor de presión de neumáticos|26 - 50|1954.68|D-66R       |6424 |
+------+--------------------------------+-------+-------+------------+-----+
```
DESPUES:
```Pyspark
+------+--------------------------------+-------+-------+------------+-----+
|sku   |nombre                          |escala |precio |categoria_id|stock|
+------+--------------------------------+-------+-------+------------+-----+
|G660F4|Inyector de combustible         |1 - 25 |4864.73|p-92w       |5528 |
|L647n5|Medidor de presion de neumaticos|26 - 50|1954.68|D-66R       |6424 |
+------+--------------------------------+-------+-------+------------+-----+
```
[Back to Top](#Contenido)

## 5. Tabla de hechos 

### 5.1 Ejercicio 1
Generar una tabla de productos expandida. Del dataset de Ecommerce utiliza la tabla de productos, detalle_productos y detalle_categoria para crear una tabla titulada productos_expandida con las siguientes columnas:

<p align="center">
 sku |marca|modelo|year|escala|*escala_min*|*escala_max*|categoria_id|categoria|stock
 </p>

Generando las columnas escala_min, escala_max
```Pyspark
df_new_product = df_clean_products.withColumn('escala_min', regexp_extract(col('escala'), r'([0-9]{1,})',1)) \
     .withColumn('escala_max', regexp_extract(col('escala'), r'([0-9]{2,}+$)',1))     
```
Resultado:
```Pyspark
+------+--------------------------------+-------+-------+------------+-----+----------+----------+
|sku   |nombre                          |escala |precio |categoria_id|stock|escala_min|escala_max|
+------+--------------------------------+-------+-------+------------+-----+----------+----------+
|G660F4|Inyector de combustible         |1 - 25 |4864.73|p-92w       |5528 |1         |25        |
|G660F4|Inyector de combustible         |26 - 50|4378.25|p-92w       |5528 |26        |50        |
|G660F4|Inyector de combustible         |51 - 75|4135.02|p-92w       |5528 |51        |75        |
|L647n5|Medidor de presion de neumaticos|1 - 25 |2171.87|D-66R       |6424 |1         |25        |
|L647n5|Medidor de presion de neumaticos|26 - 50|1954.68|D-66R       |6424 |26        |50        |
+------+--------------------------------+-------+-------+------------+-----+----------+----------+
```

Generando join  de tabla productos, detalle_productos y detalle_categoria 
```Pyspark
prod = df_new_product.alias("prod")
cat_prod = raw_category_products.alias("cat_prod")
det_prod = raw_details_products.alias("det_prod")

#InnerJoin
full_prod = prod.join(cat_prod, col('prod.categoria_id') == col('cat_prod.categoria_id'), "inner") \
                                .join(det_prod, col('prod.sku') == col('det_prod.sku'), "inner")
```
Resultado:
```Pyspark
+------+---------+-----------------+------------+-------+----------+----------+-----+-------------------------+
|sku   |marca    |modelo           |categoria_id|escala |escala_min|escala_max|stock|categoria                |
+------+---------+-----------------+------------+-------+----------+----------+-----+-------------------------+
|G660F4|Ford     |Lanos            |p-92w       |1 - 25 |1         |25        |5528 |Coupe, Convertible       |
|G660F4|Ford     |Lanos            |p-92w       |26 - 50|26        |50        |5528 |Coupe, Convertible       |
|L647n5|Honda    |Range Rover      |D-66R       |51 - 75|51        |75        |6424 |Coupe, Convertible, Sedan|
|W824g2|Ford     |Mustang          |k-59K       |1 - 25 |1         |25        |1161 |Sedan, Wagon             |
|J115g7|Chevrolet|Tacoma Access Cab|k-59K       |51 - 75|51        |75        |9976 |Sedan, Wagon             |
|O565n9|Scion    |Q3               |l-58I       |1 - 25 |1         |25        |9799 |Convertible              |
|q877d8|Toyota   |Challenger       |Q-26S       |26 - 50|26        |50        |1015 |Van/Minivan              |
+------+---------+-----------------+------------+-------+----------+----------+-----+-------------------------+
```
### 5.2 Ejercicio 2
Por otro lado, la contadora de la compañía dice que le sería más eficiente si los datos de una misma orden se encontrarán más resumidos en una única línea ya que, en algunas órdenes se realiza la compra de más de un tipo de producto y debe realizar la suma del total de cada tipo de producto que se compró en esa orden para obtener la venta total de toda la orden.  Utiliza la tabla de ordenes para generar una tabla que cumpla con lo siguiente:


1- Una columna con el order_id, donde el order_id sea único.

2- Una columna que contenga el número de productos distintos que se compraron en esa orden.

3- Una columna con el total de la venta generada por orden.

4- La fecha en que se realizó la orden.
      
 
```Pyspark
#groupby orden_id | suma precio | countdistinct sku | sum cantidad
df_full = raw_ordenes.select('orden_id','precio','cantidad','sku','fecha_orden') \
        .groupBy('orden_id','fecha_orden') \
        .agg(sum('precio').alias('total_venta'),countDistinct('sku').alias('numero_productos'),sum('cantidad').alias('total_productos')) \
        .orderBy('fecha_orden',asceding=False)
```
Resultado :
```Pyspark
+-------------+-----------+-----------+----------------+---------------+
|     orden_id|fecha_orden|total_venta|numero_productos|total_productos|
+-------------+-----------+-----------+----------------+---------------+
|ORD14Gr9Y7uVH| 2005-01-01|   20653.94|               7|            174|
|ORD14Tt5f0sQa| 2005-01-01|   19551.45|               5|            124|
|ORD76OS8S8ILi| 2005-01-02|   14404.68|               5|            258|
|ORD44OU1y0Nva| 2005-01-03|    9008.03|               3|             89|
|ORD30BN2n4ORE| 2005-01-03|     6624.6|               2|             44|
+-------------+-----------+-----------+----------------+---------------+
```

Test en orden:
```Pyspark
#verificando resultado en una orden
raw_ordenes.filter(raw_ordenes.orden_id == "ORD14Gr9Y7uVH").show(truncate=False)
+-------------+-----------+----------+------+--------+-------+-----------+-----------+-------------+
|orden_id     |sucursal_id|cliente_id|sku   |cantidad|precio |fecha_orden|fecha_envio|fecha_entrega|
+-------------+-----------+----------+------+--------+-------+-----------+-----------+-------------+
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |j237w6|5       |2099.55|2005-01-01 |2005-01-05 |2005-01-02   |
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |S380Q8|44      |3921.74|2005-01-01 |2005-01-05 |2005-01-02   |
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |j687a2|28      |2393.27|2005-01-01 |2005-01-05 |2005-01-02   |
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |Y847L7|10      |5608.0 |2005-01-01 |2005-01-05 |2005-01-02   |
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |k809M3|73      |1326.29|2005-01-01 |2005-01-05 |2005-01-02   |
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |J381T1|6       |4276.23|2005-01-01 |2005-01-05 |2005-01-02   |
|ORD14Gr9Y7uVH|C-05dv     |C-94cy    |O063f8|8       |1028.86|2005-01-01 |2005-01-05 |2005-01-02   |
+-------------+-----------+----------+------+--------+-------+-----------+-----------+-------------+
```

[Back to Top](#Contenido)
