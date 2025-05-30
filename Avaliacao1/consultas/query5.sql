WITH emissao_2010 AS (
    SELECT iso_code, SUM(emissao) AS emissao_2010
    FROM public."EmissãoPoluentes"
    WHERE ano = 2010
    GROUP BY iso_code
),
emissao_2020 AS (
    SELECT iso_code, SUM(emissao) AS emissao_2020
    FROM public."EmissãoPoluentes"
    WHERE ano = 2020
    GROUP BY iso_code
)
SELECT 
    p.nome AS pais,
    e2010.emissao_2010,
    e2020.emissao_2020,
    (e2020.emissao_2020 - e2010.emissao_2010) AS reducao_emissao
FROM emissao_2010 e2010
JOIN emissao_2020 e2020 ON e2010.iso_code = e2020.iso_code
JOIN public."Países" p ON e2010.iso_code = p.iso_code
WHERE e2020.emissao_2020 < e2010.emissao_2010
ORDER BY reducao_emissao ASC
