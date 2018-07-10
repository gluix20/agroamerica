select * from proddta.f0911
where trim(glsbl) is not null
;

select * from prodctl.f0005
--where lower(drdl01) like '%gálvez%'
where trim(drsy) = '00'
and trim(drrt) = 'ST'
;