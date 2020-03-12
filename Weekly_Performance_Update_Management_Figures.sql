--Weekly KPIs

---bookings + tracking data

with settings as (
	select
		date_trunc('week', current_date - interval '7 days')::TIMESTAMP as start_date_current_week
		,dateadd( ms,-1, date_trunc('week', current_date))::TIMESTAMP as end_date_current_week
		,'week ' || date_part(week, dateadd( ms,-1, date_trunc('week', current_date))::TIMESTAMP) || ', year ' || date_part(year, dateadd( ms,-1, date_trunc('week', current_date))::TIMESTAMP)
			|| '. Dates: ' || date_trunc('week', current_date - interval '7 days')::date || ' - ' || dateadd( ms,-1, date_trunc('week', current_date))::date as time_span1
		----
		,date_trunc('week', current_date - interval '14 days')::TIMESTAMP as start_date_last_week
		,dateadd( ms,-1, date_trunc('week', current_date - interval '7 days'))::TIMESTAMP as end_date_last_week
		,'week ' || date_part(week, dateadd( ms,-1, date_trunc('week', current_date - interval '7 days'))::TIMESTAMP) || ', year ' || date_part(year, dateadd( ms,-1, date_trunc('week', current_date - interval '7 days'))::TIMESTAMP)
			|| '. Dates: ' || date_trunc('week', current_date - interval '14 days')::date || ' - ' || dateadd( ms,-1, date_trunc('week', current_date - interval '7 days'))::date as time_span2
		----
		,date_trunc('week', current_date - interval '53 week')::TIMESTAMP as start_date_last_year
		,dateadd( ms,-1, date_trunc('week', current_date - interval '52 week'))::TIMESTAMP as end_date_last_year
		,'week ' || date_part(week, dateadd( ms,-1, date_trunc('week', current_date - interval '52 week'))::TIMESTAMP) || ', year ' || date_part(year, dateadd( ms,-1, date_trunc('week', current_date - interval '52 week'))::TIMESTAMP)
			|| '. Dates: ' || date_trunc('week', current_date - interval '53 week')::date || ' - ' || dateadd( ms,-1, date_trunc('week', current_date - interval '52 week'))::date as time_span3
)
,tracking as (
select
	distinct id
	,created_at
	,case when entrance_url ilike '%.de%' then 'de'
		when entrance_url ilike '%.nl%' then 'nl'
		when entrance_url ilike '%.at%' then 'at'
		when entrance_url ilike '%.it%' then 'it' end as country_code
	,user_uuid
from source.tracking_event
where created_at >= date_trunc('month', current_date - interval '1 year 2 month') and event_type = 'entrance_happened' and entrance_url ilike '%paulcamper%'
)
,accounting_base as (
	select
	   date
	   ,sum(amount/(tax_rate+1)) as revenue_netto
	from (
	    select a.id, a.booking_id, amount
	    ,case when firstaccepted_date between start_date_current_week and end_date_current_week then 'current_week2'
	    	when firstaccepted_date between start_date_last_week and end_date_last_week then 'last_week2'
	    	when firstaccepted_date between start_date_last_year and end_date_last_year then 'last_year2' end as date
	    ,nvl(nullif(vat_rate,0), insurance_tax_rate,case
	                                                when operation ilike '%insurance%' and country_code ilike 'de' then 0.19
	                                                when operation ilike '%insurance%' and country_code ilike 'at' then 0.11
	                                                when operation ilike '%insurance%' and country_code ilike 'nl' then 0.21 end) as tax_rate
	    ,case when operation ilike 'fee_commission' then 1 end revenue_rate
	    from legacy.accounting as a
	    inner join settings as s on 1=1
	    inner join legacy.booking_status as bs on a.booking_id = bs.booking_id and (firstaccepted_date between start_date_current_week and end_date_current_week
	    	or firstaccepted_date between start_date_last_week and end_date_last_week or firstaccepted_date between start_date_last_year and end_date_last_year)
		where user_type ilike 'pc' and operation ilike 'fee_commission'
	)
	group by 1
)
, revenue as (
select
	max(case when date ilike 'current_week2' then revenue_netto end) as current_week2
	,max(case when date ilike 'last_week2' then revenue_netto end) as last_week2
	,max(case when date ilike 'last_year2' then revenue_netto end) as last_year2
from accounting_base
)
, metrics as (
SELECT '01 - time span' as metric
union all
SELECT '03 - booking_request (total requests)' as metric
union all
SELECT '04 - booking_request (unique renters)' as metric
union all
SELECT '05 - booking_offer' as metric
union all
SELECT '06 - booking_accept' as metric
union all
SELECT '07 - booking_abort' as metric
union all
SELECT '08 - revenue' as metric
)
, base as (
select
	metric
	,case when metric ilike '01 - time span' then time_span1 end as current_week
	,case when metric ilike '01 - time span' then time_span2 end as last_week
	,case when metric ilike '01 - time span' then time_span3 end as last_year
	,case when metric ilike '08 - revenue' then current_week2 end as current_week2
	,case when metric ilike '08 - revenue' then last_week2 end as last_week2
	,case when metric ilike '08 - revenue' then last_year2 end as last_year2
	,count (distinct case when metric ilike '03 - booking_request (total requests)' and bd.first_requested_date between start_date_current_week and end_date_current_week then bd.booking_id
		when metric ilike '04 - booking_request (unique renters)' and bs.requested_unique is true  --the first booking state id taken if travel days are the same for the same renter. if the days shift is even 1-2 days, these bookings are not the same
    		and bs.requested_date between start_date_current_week and end_date_current_week then bs.booking_id
    	when metric ilike '05 - booking_offer' and bs.system_substatus in ('offer_for_request','offer_created')
			and bs.created_at_date between start_date_current_week and end_date_current_week then bs.booking_id
		when metric ilike '06 - booking_accept' and bd.first_accepted_date between start_date_current_week and end_date_current_week then bd.booking_id
		when metric ilike '07 - booking_abort' and bs.system_status ilike 'booking_aborted' and bs.created_at_date between start_date_current_week and end_date_current_week then bs.booking_id end) as current_week1
	----
	,count (distinct case when metric ilike '03 - booking_request (total requests)' and bd.first_requested_date between start_date_last_week and end_date_last_week then bd.booking_id
		when metric ilike '04 - booking_request (unique renters)' and bs.requested_unique is true  --the first booking state id taken if travel days are the same for the same renter. if the days shift is even 1-2 days, these bookings are not the same
    		and bs.requested_date between start_date_last_week and end_date_last_week then bs.booking_id
    	when metric ilike '05 - booking_offer' and bs.system_substatus in ('offer_for_request','offer_created')
			and bs.created_at_date between start_date_last_week and end_date_last_week then bs.booking_id
		when metric ilike '06 - booking_accept' and bd.first_accepted_date between start_date_last_week and end_date_last_week then bd.booking_id
		when metric ilike '07 - booking_abort' and bs.system_status ilike 'booking_aborted' and bs.created_at_date between start_date_last_week and end_date_last_week then bs.booking_id end) as last_week1
	----
	,count (distinct case when metric ilike '03 - booking_request (total requests)' and bd.first_requested_date between start_date_last_year and end_date_last_year then bd.booking_id
		when metric ilike '04 - booking_request (unique renters)' and bs.requested_unique is true  --the first booking state id taken if travel days are the same for the same renter. if the days shift is even 1-2 days, these bookings are not the same
    		and bs.requested_date between start_date_last_year and end_date_last_year then bs.booking_id
    	when metric ilike '05 - booking_offer' and bs.system_substatus in ('offer_for_request','offer_created')
			and bs.created_at_date between start_date_last_year and end_date_last_year then bs.booking_id
		when metric ilike '06 - booking_accept' and bd.first_accepted_date between start_date_last_year and end_date_last_year then bd.booking_id
		when metric ilike '07 - booking_abort' and bs.system_status ilike 'booking_aborted' and bs.created_at_date between start_date_last_year and end_date_last_year then bs.booking_id end) as last_year1
from legacy.booking_status as bs
inner join legacy.booking_details as bd on bs.booking_id = bd.booking_id
inner join settings as s on 1=1
--inner join country_setup as cs on cs.booking_id = bs.booking_id
inner join metrics m on 1=1
inner join revenue r on 1=1
group by 1,2,3,4,5,6,7
)
SELECT
	metric
	,greatest(current_week, current_week1::varchar(16), current_week2::varchar(16)) as current_week
	,greatest(last_week, last_week1::varchar(16), last_week2::varchar(16)) as last_week
	----
	,case when greatest(last_week1,last_week2) = 0 then 0
		else GREATEST(current_week1, current_week2)/GREATEST(last_week1, last_week2)::float - 1 end as anstieg
	---
	,greatest(last_year, last_year1::varchar(16), last_year2::varchar(16)) as last_year
	----
	,case when greatest(last_year1,last_year2) = 0 then 0
		else GREATEST(current_week1, current_week2)/GREATEST(last_year1, last_year2)::float - 1 end as anstieg
from base
union all
select
	'02 - total_visitors' as metric
	,(count(distinct case when created_at between start_date_current_week and end_date_current_week then user_uuid end))::varchar(16) as current_week
	,(count(distinct case when created_at between start_date_last_week and end_date_last_week then user_uuid end))::varchar(16) as last_week
	---
	,(count(distinct case when created_at between start_date_current_week and end_date_current_week then user_uuid end))
		/ (count(distinct case when created_at between start_date_last_week and end_date_last_week then user_uuid end))::float - 1 as anstieg
	---
	,(count(distinct case when created_at between start_date_last_year and end_date_last_year then user_uuid end))::varchar(16) as last_year
	---
	,(count(distinct case when created_at between start_date_current_week and end_date_current_week then user_uuid end))
		/ (count(distinct case when created_at between start_date_last_year and end_date_last_year then user_uuid end))::float - 1 as anstieg
	---
from tracking t
inner join settings s on 1=1
order by 1