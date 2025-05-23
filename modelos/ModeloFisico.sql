-- This script was generated by the ERD tool in pgAdmin 4.
-- Please log an issue at https://github.com/pgadmin-org/pgadmin4/issues/new/choose if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE IF NOT EXISTS public."Região"
(
    regiao_code character varying(4) NOT NULL,
    nome character varying(50) NOT NULL,
    PRIMARY KEY (regiao_code)
);

CREATE TABLE IF NOT EXISTS public."TipoGases"
(
    tipo_gases_id integer NOT NULL,
    nome character varying(50) NOT NULL,
    PRIMARY KEY (tipo_gases_id)
);

CREATE TABLE IF NOT EXISTS public."Países"
(
    iso_code character varying(4) NOT NULL,
    regiao_code character varying(4) NOT NULL,
    nome character varying(50) NOT NULL,
    PRIMARY KEY (iso_code)
);

CREATE TABLE IF NOT EXISTS public."Gases"
(
    gas_id integer NOT NULL,
    tipo_gas_id integer NOT NULL,
    nome character varying NOT NULL,
    PRIMARY KEY (gas_id)
);

CREATE TABLE IF NOT EXISTS public."Demografia"
(
    iso_code character varying(4) NOT NULL,
    ano integer NOT NULL,
    populacao integer,
    PRIMARY KEY (iso_code, ano)
);

CREATE TABLE IF NOT EXISTS public."IndicadoresEconômicos"
(
    iso_code character varying(4) NOT NULL,
    ano integer NOT NULL,
    pib double precision,
    PRIMARY KEY (iso_code, ano)
);

CREATE TABLE IF NOT EXISTS public."EmissãoTotalGHG"
(
    iso_code character varying(4) NOT NULL,
    ano integer NOT NULL,
    total_ghg double precision,
    total_ghg_excluding_lucf double precision,
    PRIMARY KEY (iso_code, ano)
);

CREATE TABLE IF NOT EXISTS public."MudançaTemperatura"
(
    iso_code character varying(4) NOT NULL,
    gas_id integer NOT NULL,
    ano integer NOT NULL,
    mudanca_temp double precision,
    PRIMARY KEY (iso_code, gas_id, ano)
);

CREATE TABLE IF NOT EXISTS public."EmissãoComércio"
(
    iso_code character varying NOT NULL,
    gas_id integer NOT NULL,
    ano integer NOT NULL,
    comercio_co2 double precision,
    consumo_co2 double precision,
    PRIMARY KEY (iso_code, gas_id, ano)
);

CREATE TABLE IF NOT EXISTS public."FontesEnergia"
(
    fonte_energia_id integer NOT NULL,
    nome character varying(50),
    PRIMARY KEY (fonte_energia_id)
);

CREATE TABLE IF NOT EXISTS public."FontesPoluente"
(
    fonte_poluente_id integer NOT NULL,
    nome character varying(50),
    PRIMARY KEY (fonte_poluente_id)
);

CREATE TABLE IF NOT EXISTS public."AtividadesEnergia"
(
    iso_code character varying(4) NOT NULL,
    fonte_energia_id integer NOT NULL,
    ano integer NOT NULL,
    producao double precision,
    geracao double precision,
    consumo double precision,
    PRIMARY KEY (iso_code, fonte_energia_id, ano)
);

CREATE TABLE IF NOT EXISTS public."EmissãoPoluentes"
(
    iso_code character varying(50) NOT NULL,
    gas_id integer NOT NULL,
    fonte_poluente_id integer NOT NULL,
    ano integer NOT NULL,
    emissao double precision,
    emissao_cumulativa double precision,
    PRIMARY KEY (iso_code, gas_id, fonte_poluente_id, ano)
);

ALTER TABLE IF EXISTS public."Países"
    ADD FOREIGN KEY (regiao_code)
    REFERENCES public."Região" (regiao_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Gases"
    ADD FOREIGN KEY (tipo_gas_id)
    REFERENCES public."TipoGases" (tipo_gases_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Demografia"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."IndicadoresEconômicos"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."EmissãoTotalGHG"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."MudançaTemperatura"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."MudançaTemperatura"
    ADD FOREIGN KEY (gas_id)
    REFERENCES public."Gases" (gas_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."EmissãoComércio"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."EmissãoComércio"
    ADD FOREIGN KEY (gas_id)
    REFERENCES public."Gases" (gas_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."AtividadesEnergia"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."AtividadesEnergia"
    ADD FOREIGN KEY (fonte_energia_id)
    REFERENCES public."FontesEnergia" (fonte_energia_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."EmissãoPoluentes"
    ADD FOREIGN KEY (iso_code)
    REFERENCES public."Países" (iso_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."EmissãoPoluentes"
    ADD FOREIGN KEY (gas_id)
    REFERENCES public."Gases" (gas_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."EmissãoPoluentes"
    ADD FOREIGN KEY (fonte_poluente_id)
    REFERENCES public."FontesPoluente" (fonte_poluente_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

END;