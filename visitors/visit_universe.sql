truncate table dims.visitor;

insert into dims.visitor

select user_uuid
, min(created_at) as first_visitor_date

from source.tracking_event

group by 1;



truncate table dims.v_account_registration;

insert into dims.v_account_registration

select user_uuid
, user_id
, min(created_at) as account_registration_date

from source.tracking_event

where event_type = 'account_created'

group by 1,2;



truncate table dims.v_camper_created;

insert into dims.v_camper_created

select user_uuid
,camper_id
,min(created_at) as camper_created_at

from source.tracking_event

where event_type = 'camper_created'

group by 1,2;



truncate table dims.v_booking_request;

insert into dims.v_booking_request

select user_uuid
,booking_id as booking_request_id
,min(created_at) as booking_request_created_at

from source.tracking_event

where event_type = 'booking_requested'

group by 1,2;



truncate table dims.v_potential_registration;

insert into dims.v_potential_registration

select user_uuid
, potential_registration_id as potential_registration_id
,'user_subscribed' as potential_registration_type
,min(created_at) as potential_registration_created_at

from source.tracking_event

where event_type = 'user_subscribed'

group by 1,2,3;



truncate table dims.visit;

insert into dims.visit

with raw_url_data as (
	select 
		user_uuid
		,id
		,created_at as entrance_date
		,entrance_url as entrance_url
		,referer_url as referer_url
	from source.tracking_event
	where event_type = 'entrance_happened' --and created_at >= '2020-03-17' --and user_uuid = '0011dfea-42a5-4795-91ff-e513e7208f7f'
	)
,url_visit_dates_part1 as (
select
	url.user_uuid
	, url.entrance_date
	, lead(url.entrance_date, 1) over (order by user_uuid, entrance_date) as end_date
	, lead(url.user_uuid, 1) over (order by user_uuid, entrance_date) as lead_user_uuid
from raw_url_data url
)
,url_visit_dates as (
select
	url.user_uuid
	, url.entrance_date
	, case when url.user_uuid != url.lead_user_uuid then '2099-01-01 12:00:00' else url.end_date end as end_date
from url_visit_dates_part1 url 
)

select 
raw_url_data.user_uuid
,raw_url_data.id as visit_id
,raw_url_data.entrance_date as visit_created_at
,url_visit_dates.end_date as visit_ended_at
,raw_url_data.referer_url
,raw_url_data.entrance_url
,split_part(split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1), '.',2) as domain_country_code
,case when split_part(split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1), '.',2) = 'com' then 'de' else split_part(split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1), '.',2) end as domain_country_code_modified
,entrance_url similar to '(%/wohnmobil-mieten%)|(%/camper-huren%)|(%/rent-camper%)|(%/affitta-camper%)' as url_renter
,entrance_url similar to '(%/add-camper%)|(%/wohnmobil-vermieten%)|(%/camper-verhuren%)|(%/diventa-proprietario%)' as url_lender
,split_part(split_part(entrance_url, 'utm_source=',2),'&',1) as source
,split_part(split_part(entrance_url, 'utm_medium=',2),'&',1) as medium
,split_part(split_part(entrance_url, 'utm_campaign=',2),'&',1) as campaign
,split_part(split_part(entrance_url, 'utm_content=',2),'&',1) as content
,split_part(split_part(entrance_url, 'utm_term=',2),'&',1) as term
,split_part(split_part(entrance_url, 'gclid=',2),'&',1) as gclid
FROM raw_url_data
left join url_visit_dates on(url_visit_dates.user_uuid = raw_url_data.user_uuid and url_visit_dates.entrance_date = raw_url_data.entrance_date)
where split_part(regexp_replace(entrance_url, '^http[s]?://(www\.)?|^www\.', ''), '/',1) ilike 'paul%'
;







