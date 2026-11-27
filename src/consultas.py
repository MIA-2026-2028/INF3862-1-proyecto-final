"""Consultas analíticas 4, 5 y 6 de la E1 sobre las tablas gold.

Son las del anexo A de la E1, con el período completo en lugar de :desde y
:hasta, y la clave de receta en el ranking de la consulta 6.
"""

DESDE, HASTA = "2007-12-01", "2008-02-29"   # período completo de las mediciones

CONSULTA_4 = f"""
WITH perfil AS (
  SELECT usuario_key, AVG(fc_reposo) AS fc_reposo, AVG(pasos) AS pasos,
         AVG(minutos_sesion) AS minutos, AVG(sodio) AS sodio,
         AVG(grasa_saturada) AS grasa_sat, AVG(fibra) AS fibra
  FROM hecho_usuario_dia
  WHERE fecha_key BETWEEN '{DESDE}' AND '{HASTA}'
    AND fc_reposo IS NOT NULL
  GROUP BY usuario_key),
cuartil_usuario AS (
  SELECT perfil.*, NTILE(4) OVER (ORDER BY fc_reposo) AS cuartil
  FROM perfil)
SELECT cuartil, COUNT(*) AS usuarios, ROUND(AVG(fc_reposo), 1) AS fc_reposo,
       ROUND(AVG(pasos)) AS pasos, ROUND(AVG(minutos), 1) AS minutos_sesion,
       ROUND(AVG(sodio)) AS sodio, ROUND(AVG(grasa_sat), 1) AS grasa_saturada,
       ROUND(AVG(fibra), 1) AS fibra
FROM cuartil_usuario GROUP BY cuartil ORDER BY cuartil
"""

CONSULTA_5 = """
SELECT f.semana, r.modelo, ROUND(AVG(h.pasos)) AS pasos_promedio, COUNT(*) AS filas
FROM hecho_usuario_reloj_dia h
JOIN dim_fecha f ON f.fecha_key = h.fecha_key
JOIN dim_reloj r ON r.reloj_key = h.reloj_key
GROUP BY f.semana, r.modelo ORDER BY f.semana, r.modelo
"""

CONSULTA_6 = f"""
WITH perfil AS (
  SELECT usuario_key, AVG(fc_reposo) AS fc_reposo
  FROM hecho_usuario_dia
  WHERE fecha_key BETWEEN '{DESDE}' AND '{HASTA}'
  GROUP BY usuario_key
), cuartil_usuario AS (
  SELECT usuario_key, NTILE(4) OVER (ORDER BY fc_reposo) AS cuartil
  FROM perfil WHERE fc_reposo IS NOT NULL
), conteo AS (
  SELECT p.cuartil, d.nombre, COUNT(*) AS veces,
         ROW_NUMBER() OVER (PARTITION BY p.cuartil ORDER BY COUNT(*) DESC, d.nombre) AS rn
  FROM hecho_comida h
  JOIN dim_receta d ON d.receta_key = h.receta_key
  JOIN cuartil_usuario p ON p.usuario_key = h.usuario_key
  WHERE h.fecha_key BETWEEN '{DESDE}' AND '{HASTA}'
  GROUP BY p.cuartil, d.receta_key, d.nombre
)
SELECT cuartil, nombre, veces FROM conteo WHERE rn <= 10
ORDER BY cuartil, veces DESC, nombre
"""

CONSULTAS = {"consulta_4": CONSULTA_4, "consulta_5": CONSULTA_5, "consulta_6": CONSULTA_6}
