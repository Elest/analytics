---key kpi qac
-- should be for chosen quarter (- 3 month from the chosen Month)

WITH onboarding_date as (
SELECT
    cs.camper_id
    ,min(created_at) onboarding
    ,to_char(min(created_at),'YYYY_Q')::VARCHAR(16) AS onboarding_year_q
FROM source.camper_status as cs
inner join dims.camper c on c.camper_id = cs.camper_id
WHERE cs.publishing_status ILIKE 'published' and cs.crm_status ilike 'approved' and created_at::date >= '2016-04-01'
GROUP BY 1
)
select
    count(distinct case when
        ((first_offered_date >= '2018-12-01' or (first_offered_date is null and first_accepted_date >= '2018-12-01'))
    and (first_offered_date < '2019-03-01' or (first_offered_date is null and first_accepted_date >= '2019-03-01'))) then bd.camper_id end) as qac_key_kpi_prev
    ,count(distinct case when
        ((first_offered_date >= '2019-12-01' or (first_offered_date is null and first_accepted_date >= '2019-12-01'))
    and (first_offered_date < '2020-03-01' or (first_offered_date is null and first_accepted_date >= '2020-03-01'))) then bd.camper_id end) as qac_key_kpi_current
from dims.booking as bd
inner JOIN onboarding_date AS od ON od.camper_id = bd.camper_id



--key kpi yar
-- should be for chosen year (- 12 month from the chosen Month)

with onboarding_date as (
SELECT
    renter_id
    ,country_code
    ,date_part_year(min(u.created_at::DATE))::varchar(16) AS first_requested_year
FROM dims.booking b
inner join dims.user_pseudonym u on u.user_id = b.renter_id
where first_requested_date::DATE >= '2016-01-01' and u.created_at::DATE >= '2016-01-01'
GROUP BY 1,2
having country_code is not null
)
SELECT
    count(DISTINCT case when
        first_requested_year is not null and first_requested_date::DATE >= '2018-03-01' and first_requested_date::DATE < '2019-03-01' then bd.renter_id end) AS yar_key_kpi_prev
    ,count(DISTINCT case when
        first_requested_year is not null and first_requested_date::DATE >= '2019-03-01' and first_requested_date::DATE < '2020-03-01' then bd.renter_id end) AS yar_key_kpi_current
FROM dims.booking AS bd
inner JOIN onboarding_date AS od ON od.renter_id = bd.renter_id


-- Activation Renter

--FB14

Select
    to_char(b.first_accepted_date, 'yyyy_mm') as yyyy_mm
    ,count(distinct b.renter_id) as fb14
from dims.user_pseudonym up
left join dims.booking b on (up.user_id = b.renter_id and up.created_at + interval '14 days' >= b.first_accepted_date)
where b.first_accepted_date::date between '2020-03-01' and '2020-03-31'
    or b.first_accepted_date::date between '2019-03-01' and '2019-03-31'
group by 1
order by 1


--5star_review_180

select
    to_char(rr.created_at, 'yyyy_mm') as yyyy_mm
    ,count(distinct case when rr.id is not null then up.user_id else null end) as _5Star_Review_180
from dims.user_pseudonym up
left join dims.booking b on (b.renter_id = up.user_id)
left join import.public_review rr on(rr.booking_id = b.booking_id and review_for = 'camper' and rr.author_id = b.renter_id and up.created_at + interval '180 days' > rr.created_at and rr.rating = 5)
where rr.created_at::date between '2020-03-01' and '2020-03-31'
    OR rr.created_at::date between '2019-03-01' and '2019-03-31'
group by 1
order by 1


--2ndBR_5star_180


SELECT
    to_char(b180.created_at, 'yyyy_mm') as yyyy_mm
    ,count(distinct b180.renter_id) as two_rq180
From (
    select
        up.user_id
        , up.created_at as signup_date
        , min(case when rr.id is not null then rr.created_at else null end) as first_5_star_rating_date
    from dims.user_pseudonym up
    left join dims.booking b on(b.renter_id = up.user_id)
    left join import.review_review rr on(rr.booking_id = b.booking_id and review_for = 'camper' and rr.author_id = b.renter_id and up.created_at + interval '180 days' > rr.created_at and rr.rating = 5)
    where has_camper is false
    group by 1,2
    ) up
left join dims.booking b180 on(up.user_id = b180.renter_id and up.first_5_star_rating_date < b180.created_at and up.first_5_star_rating_date + interval '180 days' >= b180.created_at)
where b180.created_at::date between '2020-03-01' and '2020-03-31'
    or b180.created_at::date between '2019-03-01' and '2019-03-31'
group by 1
order by 1



--Activation Lender

--RQ14

Select
    to_char(lender_semi.lender_semi_published_date, 'yyyy_mm') as yyyy_mm
    ,count(distinct lender_semi.camper_owner_id) as RQ14
from dims.user_pseudonym up
--
left join (
	select
		camper_owner_id
		,listagg(c.camper_id, ',') as camper_ids
		,min(semi_published_date) as lender_semi_published_date  -- first semi publishing date per lender
	from dims.camper c
	left join (
		select
			camper_id
			,min(updated_at) as semi_published_date
		from "source".camper_status
		where publishing_status not ilike 'draft' and crm_status not in ('in_review', 'blocked')
		group by 1
		) semi on(semi.camper_id = c.camper_id)
	group by 1
) lender_semi on (lender_semi.camper_owner_id = up.user_id and up.created_at + interval '14 day' >= lender_semi.lender_semi_published_date) -- camper requestable in the first 14 days after signup
--
left join (
	Select
		c.camper_owner_id
		,min(b.created_at) as first_booking_request_date -- first BR per lender
	from dims.booking b
	left join dims.camper c on (c.camper_id = b.camper_id)
	group by 1
) first_booking_per_lender on (first_booking_per_lender.camper_owner_id = lender_semi.camper_owner_id
	and lender_semi.lender_semi_published_date + interval '7 days' >= first_booking_per_lender.first_booking_request_date) -- first BR in the 7 days after being requestable
--
left join (
	select *
	from (
	Select
		c.camper_owner_id
		,b.booking_id
		,b.created_at as booking_date
		,ROW_NUMBER() OVER(PARTITION BY c.camper_owner_id ORDER BY b.booking_id) as order_no_per_camper_owner
	from dims.booking b
	left join dims.camper c on(c.camper_id = b.camper_id)
	where b.booking_sub_status in('contract_signed','offer_amended','offer_canceled','offer_created','offer_declined','offer_for_request')
	)
	where order_no_per_camper_owner = 2
) second_offer on(second_offer.camper_owner_id = lender_semi.camper_owner_id and lender_semi.lender_semi_published_date + interval '14 days' >= second_offer.booking_date)
where lender_semi.lender_semi_published_date::date between '2020-03-01' and '2020-03-31'
    or lender_semi.lender_semi_published_date::date between '2019-03-01' and '2019-03-31'
group by 1
order by 1

--FB7RQ14

Select
    to_char(first_booking_per_lender.first_booking_request_date, 'yyyy_mm') as yyyy_mm
    ,count(distinct first_booking_per_lender.camper_owner_id) as FB7RQ14
from dims.user_pseudonym up
--
left join (
	select
		camper_owner_id
		,listagg(c.camper_id, ',') as camper_ids
		,min(semi_published_date) as lender_semi_published_date  -- first semi publishing date per lender
	from dims.camper c
	left join (
		select
			camper_id
			,min(updated_at) as semi_published_date
		from "source".camper_status
		where publishing_status not ilike 'draft' and crm_status not in ('in_review', 'blocked')
		group by 1
		) semi on(semi.camper_id = c.camper_id)
	group by 1
) lender_semi on (lender_semi.camper_owner_id = up.user_id and up.created_at + interval '14 day' >= lender_semi.lender_semi_published_date) -- camper requestable in the first 14 days after signup
--
left join (
	Select
		c.camper_owner_id
		,min(b.created_at) as first_booking_request_date -- first BR per lender
	from dims.booking b
	left join dims.camper c on (c.camper_id = b.camper_id)
	group by 1
) first_booking_per_lender on (first_booking_per_lender.camper_owner_id = lender_semi.camper_owner_id
	and lender_semi.lender_semi_published_date + interval '7 days' >= first_booking_per_lender.first_booking_request_date) -- first BR in the 7 days after being requestable
--
left join (
	select *
	from (
	Select
		c.camper_owner_id
		,b.booking_id
		,b.created_at as booking_date
		,ROW_NUMBER() OVER(PARTITION BY c.camper_owner_id ORDER BY b.booking_id) as order_no_per_camper_owner
	from dims.booking b
	left join dims.camper c on(c.camper_id = b.camper_id)
	where b.booking_sub_status in('contract_signed','offer_amended','offer_canceled','offer_created','offer_declined','offer_for_request')
	)
	where order_no_per_camper_owner = 2
) second_offer on(second_offer.camper_owner_id = lender_semi.camper_owner_id and lender_semi.lender_semi_published_date + interval '14 days' >= second_offer.booking_date)
where first_booking_per_lender.first_booking_request_date::date between '2020-03-01' and '2020-03-31'
    or first_booking_per_lender.first_booking_request_date::date between '2019-03-01' and '2019-03-31'
group by 1
order by 1


--_2o14RQ14

Select
    to_char(second_offer.booking_date, 'yyyy_mm') as yyyy_mm
    ,count(distinct second_offer.camper_owner_id) as _2o14RQ14
from dims.user_pseudonym up
--
left join (
	select
		camper_owner_id
		,listagg(c.camper_id, ',') as camper_ids
		,min(semi_published_date) as lender_semi_published_date  -- first semi publishing date per lender
	from dims.camper c
	left join (
		select
			camper_id
			,min(updated_at) as semi_published_date
		from "source".camper_status
		where publishing_status not ilike 'draft' and crm_status not in ('in_review', 'blocked')
		group by 1
		) semi on(semi.camper_id = c.camper_id)
	group by 1
) lender_semi on (lender_semi.camper_owner_id = up.user_id and up.created_at + interval '14 day' >= lender_semi.lender_semi_published_date) -- camper requestable in the first 14 days after signup
--
left join (
	Select
		c.camper_owner_id
		,min(b.created_at) as first_booking_request_date -- first BR per lender
	from dims.booking b
	left join dims.camper c on (c.camper_id = b.camper_id)
	group by 1
) first_booking_per_lender on (first_booking_per_lender.camper_owner_id = lender_semi.camper_owner_id
	and lender_semi.lender_semi_published_date + interval '7 days' >= first_booking_per_lender.first_booking_request_date) -- first BR in the 7 days after being requestable
--
left join (
	select *
	from (
	Select
		c.camper_owner_id
		,b.booking_id
		,b.created_at as booking_date
		,ROW_NUMBER() OVER(PARTITION BY c.camper_owner_id ORDER BY b.booking_id) as order_no_per_camper_owner
	from dims.booking b
	left join dims.camper c on(c.camper_id = b.camper_id)
	where b.booking_sub_status in('contract_signed','offer_amended','offer_canceled','offer_created','offer_declined','offer_for_request')
	)
	where order_no_per_camper_owner = 2
) second_offer on(second_offer.camper_owner_id = lender_semi.camper_owner_id and lender_semi.lender_semi_published_date + interval '14 days' >= second_offer.booking_date)
where second_offer.booking_date::date between '2020-03-01' and '2020-03-31'
    or second_offer.booking_date::date between '2019-03-01' and '2019-03-31'
group by 1
order by 1


-- yellow flags

select
	country_code
    ,count(distinct camper_id)
from (
	select
		user_id
		,max(case when block_status ilike 'yellow' then last_update end) as yellow
		,max(case when block_status ilike 'none' then last_update end) as none
	from (
		select
			user_id
			,block_status
			,max(created_at) as last_update
		from source.user_status us
		where created_at::date <= '2020-03-31'
		group by 1,2
		) u
	group by 1
	having (yellow > none or none is null) and yellow is not null
	) u
inner join dims.camper c on c.camper_owner_id = u.user_id
group by 1
order by 1

