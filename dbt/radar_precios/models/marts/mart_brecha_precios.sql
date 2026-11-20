{{
  config(materialized='table')
}}

/*
  Ranking de brecha de precio entre comercios, por producto (EAN) y día.
  A diferencia de mart_precio_por_kilo, compara precio_lista directo: al
  ser el mismo EAN (mismo empaque exacto) no hace falta normalizar por
  kilo para que la comparación sea justa.

  Filtros (configurables en dbt_project.yml, no hardcodeados):
    - mismo dt Y fecha_archivo dentro de max_lag_days_brecha días de ese
      dt - comparar el precio de hoy de un comercio contra el de hace una
      semana de otro no es una "brecha" real, es ruido de frescura.
    - excluye filas es_outlier=true (ver stg_productos).
    - exige al menos min_comercios_brecha comercios por producto - una
      "brecha" entre 2 comercios nomás no sostiene un ranking.
*/

with base as (

    select *
    from {{ ref('stg_productos') }}
    where not es_outlier
      and fecha_archivo is not null
      and date_diff(dt, fecha_archivo, day) between 0 and {{ var('max_lag_days_brecha') }}

),

por_comercio as (

    select
        dt,
        id_producto,
        id_comercio,
        any_value(productos_descripcion) as productos_descripcion,
        avg(precio_lista) as precio_comercio

    from base
    group by dt, id_producto, id_comercio

),

agregado as (

    select
        dt,
        id_producto,
        any_value(productos_descripcion) as productos_descripcion,
        count(distinct id_comercio) as n_comercios,
        array_agg(
            struct(id_comercio, precio_comercio) order by precio_comercio asc limit 1
        )[offset(0)] as min_registro,
        array_agg(
            struct(id_comercio, precio_comercio) order by precio_comercio desc limit 1
        )[offset(0)] as max_registro

    from por_comercio
    group by dt, id_producto
    having n_comercios >= {{ var('min_comercios_brecha') }}

)

select
    to_hex(md5(concat(cast(dt as string), '|', id_producto))) as producto_dia_id,
    dt,
    id_producto,
    productos_descripcion,
    n_comercios,
    min_registro.id_comercio as comercio_mas_barato,
    min_registro.precio_comercio as precio_min,
    max_registro.id_comercio as comercio_mas_caro,
    max_registro.precio_comercio as precio_max,
    max_registro.precio_comercio - min_registro.precio_comercio as brecha_abs,
    safe_divide(
        max_registro.precio_comercio - min_registro.precio_comercio, min_registro.precio_comercio
    ) as brecha_pct,
    row_number() over (
        partition by dt
        order by safe_divide(
            max_registro.precio_comercio - min_registro.precio_comercio, min_registro.precio_comercio
        ) desc
    ) as ranking_brecha

from agregado
