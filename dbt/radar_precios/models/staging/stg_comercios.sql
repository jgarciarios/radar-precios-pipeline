{{
  config(materialized='view')
}}

/*
  Staging de comercios: una fila por (dt, id_comercio), no por
  (dt, id_comercio, id_bandera) como la fuente. Un comercio puede tener
  más de una bandera (ej. comercio 24: banderas "SIMPLICITY" y
  "FARMACITY", misma razón social "Farmcity S.A.") - comercio_razon_social
  y comercio_cuit son del mismo tal fiscal así que con any_value() alcanza;
  los nombres de bandera se guardan como array para no perder esa
  información, no porque haga falta para el join con productos (que es
  por id_comercio, no por bandera).
*/

select
    to_hex(md5(concat(cast(dt as string), '|', id_comercio))) as comercio_id_dia,
    dt,
    id_comercio,
    any_value(comercio_cuit) as comercio_cuit,
    any_value(comercio_razon_social) as comercio_razon_social,
    array_agg(distinct comercio_bandera_nombre) as banderas,
    count(distinct id_bandera) as cantidad_banderas,
    max(safe_cast(comercio_ultima_actualizacion as timestamp)) as ultima_actualizacion

from {{ source('sepa', 'comercio') }}
group by dt, id_comercio
