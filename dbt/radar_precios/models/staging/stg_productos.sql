{{
  config(materialized='view')
}}

/*
  Staging de productos: tipos correctos, fecha_archivo a DATE, unidad de
  presentación normalizada, precio_por_kilo calculado a mano (NO desde
  productos_precio_referencia - ver README, sección "por qué no usamos
  precio_referencia", caso del whisky de 750cc) y es_outlier marcado sin
  borrar la fila.

  Grano: una fila por (dt, id_comercio, id_bandera, id_sucursal,
  id_producto) - acá no se agrega nada todavía (eso es trabajo de los
  marts). id_bandera entra en la clave porque un mismo comercio puede
  tener más de una bandera que reutiliza el mismo rango de id_sucursal
  (visto en comercio 6: sucursal "1" existe bajo las 2 banderas). Incluso
  así, 49 filas de comercio 5 quedan duplicadas EXACTAS (mismo precio,
  misma descripción, todos los campos iguales) - un bug real del export de
  ese comercio, no un problema de la clave. Se dedupean con QUALIFY al
  final en vez de ignorar el test de unicidad.
*/

with base as (

    select
        dt,
        safe.parse_date('%Y-%m-%d', fecha_archivo) as fecha_archivo,
        id_comercio,
        id_bandera,
        id_sucursal,
        id_producto,
        productos_descripcion,
        productos_marca,
        safe_cast(productos_cantidad_presentacion as float64) as cantidad_presentacion,
        case
            when upper(trim(productos_unidad_medida_presentacion)) in ('GR', 'GR.', 'GRM') then 'GR'
            when upper(trim(productos_unidad_medida_presentacion)) in ('KG', 'KG.', 'KGM', 'KGR') then 'KG'
            when upper(trim(productos_unidad_medida_presentacion)) in ('ML', 'ML.', 'CM3') then 'ML'
            when upper(trim(productos_unidad_medida_presentacion)) in ('L', 'LT', 'LT.', 'LTR') then 'LT'
            when upper(trim(productos_unidad_medida_presentacion)) in ('UNI', 'UD', 'UNIDAD', 'UN.', 'EA', 'CU', 'PCK') then 'UN'
            else 'OTRO'
        end as unidad_presentacion,
        productos_precio_lista as precio_lista,
        -- se conserva para auditoría/debug, pero ningún modelo de acá en
        -- adelante confía en este campo para normalizar precios.
        productos_precio_referencia as precio_referencia_fuente

    from {{ source('sepa', 'productos') }}

),

con_mediana as (

    select
        *,
        percentile_cont(precio_lista, 0.5)
            over (partition by dt, id_producto)
            as mediana_precio_lista_ean

    from base

)

select
    to_hex(md5(concat(
        cast(dt as string), '|', id_comercio, '|', id_bandera, '|', id_sucursal, '|', id_producto
    ))) as producto_sucursal_id,
    dt,
    fecha_archivo,
    id_comercio,
    id_bandera,
    id_sucursal,
    id_producto,
    productos_descripcion,
    productos_marca,
    cantidad_presentacion,
    unidad_presentacion,
    precio_lista,
    precio_referencia_fuente,
    case
        when unidad_presentacion = 'KG' and cantidad_presentacion > 0
            then precio_lista / cantidad_presentacion
        when unidad_presentacion = 'GR' and cantidad_presentacion > 0
            then precio_lista / (cantidad_presentacion / 1000)
        else null
    end as precio_por_kilo,
    -- precio <= 0 siempre es outlier; si no, se compara contra la mediana
    -- del mismo EAN ese mismo día (var outlier_ratio_threshold, default 3x).
    case
        when precio_lista <= 0 then true
        when mediana_precio_lista_ean <= 0 then false
        when precio_lista > mediana_precio_lista_ean * {{ var('outlier_ratio_threshold') }} then true
        when precio_lista < mediana_precio_lista_ean / {{ var('outlier_ratio_threshold') }} then true
        else false
    end as es_outlier

from con_mediana
-- dedupe de las 49 filas exactas duplicadas de comercio 5 (ver comentario
-- arriba). Da igual cuál de las dos copias idénticas queda.
qualify row_number() over (
    partition by dt, id_comercio, id_bandera, id_sucursal, id_producto
    order by id_producto
) = 1
