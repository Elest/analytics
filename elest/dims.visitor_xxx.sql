--dims.visitor_monthly
with visitor_date as (
	select user_uuid
	, min(created_at) as first_visitor_date
	from source.tracking_event
	group by 1
)
, raw_data as (
	select
		user_uuid
		,ga_user_id
		,created_at
		,entrance_url
	from source.tracking_event
	where event_type ilike 'entrance_happened'
	)
	select
		rd.user_uuid
		,ga_user_id
	    ,vd.first_visitor_date
		,to_char(created_at, 'YYYY-MM') as created_at_yearmonth
		,split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1) as entrance_domain
	    ,split_part(regexp_replace(entrance_domain, 'paulcamper.', ''), '/',1) as domain_country_code
		,entrance_url similar to '(%/wohnmobil-mieten%)|(%/camper-huren%)|(%/rent-camper%)|(%/affitta-camper%)' as url_mieten
		,entrance_url similar to '(%/add-camper%)|(%/wohnmobil-vermieten%)|(%/camper-verhuren%)|(%/diventa-proprietario%)' as url_vermieten
		,split_part(split_part(entrance_url, 'utm_source=',2),'&',1) as Source
		,split_part(split_part(entrance_url, 'utm_medium=',2),'&',1) as medium
		,split_part(split_part(entrance_url, 'utm_campaign=',2),'&',1) as campaign
		,entrance_url as EntranceURL
	FROM raw_data rd
    inner join visitor_date vd on vd.user_uuid = rd.user_uuid
where entrance_domain ilike 'paul%';




--dims.visitor_weekly
with visitor_date as (
	select user_uuid
	, min(created_at) as first_visitor_date
	from source.tracking_event
	group by 1
)
, raw_data as (
	select
		user_uuid
		,ga_user_id
		,created_at
		,entrance_url
	from source.tracking_event
	where event_type ilike 'entrance_happened'
	)
	select
		rd.user_uuid
		,ga_user_id
	    ,vd.first_visitor_date
		,to_char(created_at, 'YYYY-WW') as created_at_yearweek
		,split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1) as entrance_domain
	    ,split_part(regexp_replace(entrance_domain, 'paulcamper.', ''), '/',1) as domain_country_code
		,entrance_url similar to '(%/wohnmobil-mieten%)|(%/camper-huren%)|(%/rent-camper%)|(%/affitta-camper%)' as url_mieten
		,entrance_url similar to '(%/add-camper%)|(%/wohnmobil-vermieten%)|(%/camper-verhuren%)|(%/diventa-proprietario%)' as url_vermieten
		,split_part(split_part(entrance_url, 'utm_source=',2),'&',1) as Source
		,split_part(split_part(entrance_url, 'utm_medium=',2),'&',1) as medium
		,split_part(split_part(entrance_url, 'utm_campaign=',2),'&',1) as campaign
		,entrance_url as EntranceURL
	FROM raw_data rd
    inner join visitor_date vd on vd.user_uuid = rd.user_uuid
where entrance_domain ilike 'paul%';





--dims.visitor_daily
with visitor_date as (
	select user_uuid
	, min(created_at) as first_visitor_date
	from source.tracking_event
	group by 1
)
, raw_data as (
	select
		user_uuid
		,ga_user_id
		,created_at
		,entrance_url
	from source.tracking_event
	where event_type ilike 'entrance_happened'
	)
	select
		rd.user_uuid
		,ga_user_id
	    ,vd.first_visitor_date
		,to_char(created_at, 'YYYY-MM-DD') as created_at_date
		,split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1) as entrance_domain
	    ,split_part(regexp_replace(entrance_domain, 'paulcamper.', ''), '/',1) as domain_country_code
	    ,entrance_url similar to '(%/wohnmobil-mieten%)|(%/camper-huren%)|(%/rent-camper%)|(%/affitta-camper%)' as url_mieten
		,entrance_url similar to '(%/add-camper%)|(%/wohnmobil-vermieten%)|(%/camper-verhuren%)|(%/diventa-proprietario%)' as url_vermieten
		,split_part(split_part(entrance_url, 'utm_source=',2),'&',1) as Source
		,split_part(split_part(entrance_url, 'utm_medium=',2),'&',1) as medium
		,split_part(split_part(entrance_url, 'utm_campaign=',2),'&',1) as campaign
		,entrance_url as EntranceURL
	FROM raw_data rd
    inner join visitor_date vd on vd.user_uuid = rd.user_uuid
where entrance_domain ilike 'paul%';
