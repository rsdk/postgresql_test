CREATE OR REPLACE FUNCTION new_transfer(BIGINT, NUMERIC, FLOAT, FLOAT, VARCHAR(2), VARCHAR(64)) RETURNS VOID 

DECLARE
	current_t TIMESTAMP := localtimestamp;
	minusOneD TIMESTAMP := current_t - interval '24 hours';
	minusOneM TIMESTAMP := current_t - interval '30 days';
	decimal delta_lat := 0;
	decimal delta_long := 0;
	decimal distance := 0;
	interval delta_time;
	
	--Typ für Kartendaten
	TYPE t_card IS RECORD (
		blocked card.blocked%TYPE,
		daily_limit card.daily_limit%TYPE,
		monthly_limit card.monthly_limit%TYPE,
		distance_per_hour_max card.distance_per_hour_max%TYPE);
	v_card t_card;
	--Typ für Country Specific
	TYPE t_country_specific IS RECORD (
		disallowed country_specific.disallowed%TYPE,
		daily_limit country_specific.daily_limit%TYPE);
	v_country_specific t_country_specific;
	--Typ für Country Specific per Card
	TYPE t_country_specific_per_card IS RECORD (
		disallowed country_specific_per_card.disallowed%TYPE,
		daily_limit country_specific_per_card.daily_limit%TYPE);
	v_country_specific_per_card t_country_specific_per_card;
	--Typ für transfer geo-coordinates
	TYPE t_transfer_geo IS RECORD (
		transfer_time transfer.transfer_time%TYPE,
		latitude transfer.latitude%TYPE
		longitude transfer.longitude%TYPE);
	v_transfer_geo t_transfer_geo;
	--Variablen für Summen
	v_sum_country_daily DECIMAL := 0;
	v_sum_daily DECIMAL := 0;
	v_sum_monthly DECIMAL := 0;

BEGIN
	SELECT blocked, daily_limit, monthly_limit, distance_per_hour_max INTO v_card FROM card WHERE card_num = $1;
	SELECT disallowed, daily_limit INTO v_country_specific FROM country_specific WHERE country_code = $5;
	SELECT disallowed, daily_limit INTO v_country_specific_per_card FROM country_specific_per_card WHERE card_num = $1 AND country_code = $5;

	
	SELECT TOP 1 transfer_time, latitude, longitude INTO v_transfer_geo FROM transfer WHERE card_num = $1 ORDER BY transfer_time, card_num DESC;
	SELECT SUM(amount) INTO v_sum_country_daily FROM transfer WHERE card_num = $1 AND country_code = $5 AND transfer_time > minusOneD;
	SELECT sum(amount) INTO v_sum_daily FROM transfer WHERE card_num = $1 AND transfer_time > minusOneD;
	SELECT sum(amount) INTO v_sum_monthly FROM transfer WHERE card_num = $1 AND transfer_time > minusOneM;

	IF v_card.blocked = false THEN
	--Card is not blocked
		IF $2 > v_sum_daily THEN
		--daily amount is less than allowed
			IF $2 > v_sum_monthly THEN
			-- monthly amount is less than allowed
				IF $2 > v_sum_country_daily THEN
				-- country specific data and is not too high
					-- distance check $3 lat; $4 long
					IF v_transfer_geo.latitude > 0 THEN
						delta_lat = abs($3 - v_transfer_geo.latitude);
						delta_long = abs($4 - v_transfer_geo.longitude);
						distance = sqrt( power(delta_lat, 2) + power(delta_long, 2) );
						
						delta_time = age( current_t, v_transfer_geo.transfer_time);
						
						IF ( distance / delta_time <= v_card.distance_per_hour_max ) THEN
							INSERT INTO transfer VALUES (current_t, $1, $2, $6, $3, $4, $5);
						END IF;
					ELSE
						INSERT INTO transfer VALUES (current_t, $1, $2, $6, $3, $4, $5);
					END IF;
				END IF;
			END IF;
		END IF;


	END IF;

	
END;