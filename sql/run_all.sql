create schema if not exists stg;
create schema if not exists core;

drop table if exists stg.tmja_rrnc_2024;

create table stg.tmja_rrnc_2024 (
  datereferer text,
  rout text,
  longue text,
  prd text,
  depprd text,
  concessiond text,
  absd text,
  cumud text,
  xd text,
  yd text,
  zd text,
  prf text,
  depprf text,
  concessionf text,
  absf text,
  cumuf text,
  xf text,
  yf text,
  zf text,
  cot text,
  anneemesure text,
  typecomptage text,
  typecomptagetraff text,
  tm text,
  ratio text
);

\copy stg.tmja_rrnc_2024 FROM '../data/tmja-rrnc-2024.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"');

drop table if exists core.tmja_rrnc_2024;

create table core.tmja_rrnc_2024 as
select
  nullif(replace(longue, ',', '.'), '')::double precision as longue_m,
  trim(depprd) as dep_code,
  nullif(replace(tm, ',', '.'), '')::double precision as tmja
from stg.tmja_rrnc_2024;

drop table if exists core.exposure_dept_2024;

create table core.exposure_dept_2024 as
select
  dep_code,
  sum(tmja * (longue_m / 1000.0) * 365) as veh_km_year
from core.tmja_rrnc_2024
where dep_code is not null
  and tmja is not null
  and longue_m is not null
group by dep_code;

drop table if exists stg.baac_caract_2024;

create table stg.baac_caract_2024 (
  Num_Acc text,
  jour text,
  mois text,
  an text,
  hrmn text,
  lum text,
  dep text,
  com text,
  agg text,
  int text,
  atm text,
  col text,
  adr text,
  lat text,
  long text
);

\encoding UTF8
\copy stg.baac_caract_2024 FROM '../data/caract-2024_utf8.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"');

drop table if exists core.caract_2024;

create table core.caract_2024 as
select
  nullif(Num_Acc,'')::bigint as num_acc,
  lpad(trim(dep), 2, '0') as dep_code
from stg.baac_caract_2024
where Num_Acc is not null
  and dep is not null;

drop table if exists core.accidents_dept_2024;

create table core.accidents_dept_2024 as
select
  dep_code,
  count(*) as accidents
from core.caract_2024
group by dep_code;

drop table if exists stg.baac_usagers_2024;

create table stg.baac_usagers_2024 (
  Num_Acc text,
  id_usager text,
  id_vehicule text,
  num_veh text,
  place text,
  catu text,
  grav text,
  sexe text,
  an_nais text,
  trajet text,
  secu1 text,
  secu2 text,
  secu3 text,
  locp text,
  actp text,
  etatp text
);

\encoding UTF8
\copy stg.baac_usagers_2024 FROM '../data/usagers-2024_clean.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"');

drop table if exists core.severity_dept_2024;

create table core.severity_dept_2024 as
select
  c.dep_code,
  count(*) filter (where u.grav = '2') as morts,
  count(*) filter (where u.grav = '3') as graves,
  count(*) filter (where u.grav = '4') as legers
from core.caract_2024 c
join stg.baac_usagers_2024 u
  on c.num_acc = nullif(u.Num_Acc,'')::bigint
group by c.dep_code;

drop table if exists core.risk_severity_dept_2024;

create table core.risk_severity_dept_2024 as
select
  e.dep_code,
  a.accidents,
  s.morts,
  s.graves,
  s.legers,
  e.veh_km_year,
  (a.accidents / nullif(e.veh_km_year,0)) * 1e8 as accident_rate,
  ((5*s.morts + 2*s.graves + s.legers) / nullif(e.veh_km_year,0)) * 1e8 as severity_score
from core.exposure_dept_2024 e
join core.accidents_dept_2024 a on a.dep_code = e.dep_code
join core.severity_dept_2024 s on s.dep_code = e.dep_code;

\copy (select * from core.risk_severity_dept_2024) to '../data/risk_dept_2024_final.csv' with (format csv, header true);
