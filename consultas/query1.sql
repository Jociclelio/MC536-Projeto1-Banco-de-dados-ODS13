SELECT 
    e.ano,
    f.nome AS fonte_poluente,
    SUM(e.emissao) AS total_emissao
FROM public."EmissãoPoluentes" e
JOIN public."FontesPoluente" f ON e.fonte_poluente_id = f.fonte_poluente_id
WHERE e.iso_code = 'BRA' AND e.ano BETWEEN 2000 AND 2020
GROUP BY e.ano, f.nome
ORDER BY e.ano, f.nome;
