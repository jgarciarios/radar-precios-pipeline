{{
  config(materialized='table')
}}

/*
  Precio por kilo normalizado, por producto (EAN) y comercio, por día.
  Un comercio puede tener varias sucursales con precios distintos para el
  mismo producto - se promedia entre sucursales, no se elige una al azar.

  Solo tiene precio_por_kilo_promedio cuando el producto se vende en
  gramos/kilos (unidad_presentacion GR/KG, ver stg_productos) - comparar
  "por kilo" un producto que se vende por unidad o por litro no tiene
  sentido, así que esas filas quedan afuera en vez de forzar un número
  que no significa nada.

  Excluye filas con es_outlier=true (precio <= 0 o > 3x/< 1/3x la mediana
  del mismo EAN ese día) - la fila original sigue existiendo en
  stg_productos, acá solo no entra al promedio.
*/

with filtrado as (

    select *
    from {{ ref('stg_productos') }}
    where not es_outlier
      and precio_por_kilo is not null

)

select
    to_hex(md5(concat(
        cast(f.dt as string), '|', f.id_producto, '|', f.id_comercio
    ))) as producto_comercio_dia,
    f.dt,
    f.id_producto,
    f.id_comercio,
    c.comercio_razon_social,
    any_value(f.productos_descripcion) as productos_descripcion,
    any_value(f.productos_marca) as productos_marca,
    any_value(f.unidad_presentacion) as unidad_presentacion,
    count(*) as n_sucursales,
    avg(f.precio_lista) as precio_lista_promedio,
    avg(f.precio_por_kilo) as precio_por_kilo_promedio

from filtrado f
left join {{ ref('stg_comercios') }} c
    on c.id_comercio = f.id_comercio and c.dt = f.dt

group by f.dt, f.id_producto, f.id_comercio, c.comercio_razon_social
