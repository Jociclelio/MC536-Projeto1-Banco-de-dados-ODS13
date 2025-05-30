SELECT 
    p.nome AS pais,
    SUM(CASE WHEN fe.nome IN ('Hydro', 'Solar', 'Wind', 'Biofuel') THEN a.consumo ELSE 0 END) AS consumo_renovavel,
    SUM(CASE WHEN fe.nome IN ('Coal', 'Oil', 'Gas') THEN a.consumo ELSE 0 END) AS consumo_nao_renovavel
FROM public."AtividadesEnergia" a
JOIN public."Países" p ON a.iso_code = p.iso_code
JOIN public."FontesEnergia" fe ON a.fonte_energia_id = fe.fonte_energia_id
WHERE a.ano = 2020
GROUP BY p.nome
ORDER BY consumo_renovavel DESC
