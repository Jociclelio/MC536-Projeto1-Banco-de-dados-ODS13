SELECT 
    p.nome AS pais,
    SUM(e.emissao) AS total_emissao
FROM public."EmissãoPoluentes" e
JOIN public."Países" p ON e.iso_code = p.iso_code
WHERE e.ano = 2020
GROUP BY p.nome
ORDER BY total_emissao DESC
