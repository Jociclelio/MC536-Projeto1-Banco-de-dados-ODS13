SELECT 
    r.nome AS regiao,
    SUM(e.emissao) AS total_emissao
FROM public."EmissãoPoluentes" e
JOIN public."Países" p ON e.iso_code = p.iso_code
JOIN public."Região" r ON p.regiao_code = r.regiao_code
WHERE e.ano = 2020
GROUP BY r.nome
ORDER BY total_emissao DESC;
