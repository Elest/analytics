-- Drop table

-- DROP TABLE dims.visitor;

DROP TABLE IF EXISTS dims.visitor;
CREATE TABLE IF NOT EXISTS dims.visitor
(
	user_uuid VARCHAR(36)   ENCODE lzo
	,first_visitor_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE KEY
 DISTKEY (user_uuid)
;
ALTER TABLE dims.visitor owner to airflow;

-- Permissions

GRANT ALL ON TABLE dims.visitor TO airflow;
GRANT ALL ON TABLE dims.visitor TO group analysts;
GRANT ALL ON TABLE dims.visitor TO metabase;



-- Drop table

-- DROP TABLE dims.v_account_registration;
--event.type = 'account_created'

DROP TABLE dims.v_account_registration;
CREATE TABLE IF NOT EXISTS dims.v_account_registration
(
	user_uuid VARCHAR(36)   ENCODE lzo
	,account_id BIGINT   ENCODE lzo
	,account_registration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE KEY
 DISTKEY (user_uuid)
;
ALTER TABLE dims.v_account_registration owner to airflow;

-- Permissions

GRANT ALL ON TABLE dims.v_account_registration TO airflow;
GRANT ALL ON TABLE dims.v_account_registration TO group analysts;
GRANT ALL ON TABLE dims.v_account_registration TO metabase;



-- Drop table

-- DROP TABLE dims.v_camper_created;
--event.type = 'camper_created'

DROP TABLE dims.v_camper_created;
CREATE TABLE IF NOT EXISTS dims.v_camper_created
(
	user_uuid VARCHAR(36)   ENCODE lzo
	,camper_id BIGINT   ENCODE RAW
	,camper_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE KEY
 DISTKEY (user_uuid)
;
ALTER TABLE dims.v_camper_created owner to airflow;

-- Permissions

GRANT ALL ON TABLE dims.v_camper_created TO airflow;
GRANT ALL ON TABLE dims.v_camper_created TO group analysts;
GRANT ALL ON TABLE dims.v_camper_created TO metabase;



-- Drop table

-- DROP TABLE dims.v_booking_request;
--event.type = 'booking_requested'

DROP TABLE dims.v_booking_request;
CREATE TABLE IF NOT EXISTS dims.v_booking_request
(
	user_uuid VARCHAR(36)   ENCODE lzo
	,booking_request_id BIGINT   ENCODE RAW
	,booking_request_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE KEY
 DISTKEY (user_uuid)
;
ALTER TABLE dims.v_booking_request owner to airflow;

-- Permissions

GRANT ALL ON TABLE dims.v_booking_request TO airflow;
GRANT ALL ON TABLE dims.v_booking_request TO group analysts;
GRANT ALL ON TABLE dims.v_booking_request TO metabase;



-- Drop table

-- DROP TABLE dims.v_potential_registration;
--event.type = 'user_subscribed'

DROP TABLE dims.v_potential_registration;
CREATE TABLE IF NOT EXISTS dims.v_potential_registration
(
	user_uuid VARCHAR(36)   ENCODE lzo
	,potential_registration_id VARCHAR(100)   ENCODE lzo
	,potential_registration_type varchar(100)   ENCODE RAW
	,potential_registration_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE KEY
 DISTKEY (user_uuid)
;
ALTER TABLE dims.v_potential_registration owner to airflow;

-- Permissions

GRANT ALL ON TABLE dims.v_potential_registration TO airflow;
GRANT ALL ON TABLE dims.v_potential_registration TO group analysts;
GRANT ALL ON TABLE dims.v_potential_registration TO metabase;



-- Drop table

-- DROP TABLE dims.visit;
--event.type = 'entrance_happened'
--use lead function in order to calculate visit_ended_at (which is equal to start date of next visit per user_uuid)


DROP TABLE dims.visit;
CREATE TABLE IF NOT EXISTS dims.visit
(
	user_uuid VARCHAR(36)   ENCODE lzo
	,visit_id BIGINT ENCODE RAW
	,visit_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,visit_ended_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,referer_url VARCHAR(2100)   ENCODE lzo
	,entrance_url VARCHAR(2100)   ENCODE lzo
	,domain_country_code VARCHAR(3)   ENCODE lzo
	,domain_country_code_modified VARCHAR(3)   ENCODE lzo
	,url_renter BOOLEAN   ENCODE RAW
	,url_lender BOOLEAN   ENCODE RAW
	,source VARCHAR(2100)   ENCODE lzo
	,medium VARCHAR(2100)   ENCODE lzo
	,campaign VARCHAR(2100)   ENCODE lzo
	,content VARCHAR(2100)   ENCODE lzo
	,term VARCHAR(2100)   ENCODE lzo
	,gclid VARCHAR(200)   ENCODE lzo
)
DISTSTYLE KEY
 DISTKEY (user_uuid)
;
ALTER TABLE dims.visit owner to airflow;

-- Permissions

GRANT ALL ON TABLE dims.visit TO airflow;
GRANT ALL ON TABLE dims.visit TO group analysts;
GRANT ALL ON TABLE dims.visit TO metabase;




---NEXT ITERATION - PLEASE IMPLEMENT LOGIN_HAPPENED Table
