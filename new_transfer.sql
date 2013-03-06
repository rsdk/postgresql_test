CREATE OR REPLACE FUNCTION new_transfer(BIGINT, NUMERIC, FLOAT, FLOAT, VARCHAR, VARCHAR) RETURNS VOID AS $$
DECLARE 
	current_t TIMESTAMP := localtimestamp;
	minusOneD TIMESTAMP := current_t - interval '24 hours';
	minusOneM TIMESTAMP := current_t - interval '30 days';
	delta_lat decimal := 0;
	delta_long decimal := 0;
	distance decimal := 0;
	delta_time interval;
	
	--Kartendaten
	v_blocked card.blocked%TYPE;
	v_daily_limit card.daily_limit%TYPE;
	v_monthly_limit card.monthly_limit%TYPE;
	v_distance_per_hour_max card.distance_per_hour_max%TYPE;

	--Country Specific
	v_c_disallowed country_specific.disallowed%TYPE;
	v_c_daily_limit country_specific.daily_limit%TYPE;
	
	--Typ für Country Specific per Card
	v_cc_disallowed country_specific_per_card.disallowed%TYPE;
	v_cc_daily_limit country_specific_per_card.daily_limit%TYPE;
	
	--Typ für transfer geo-coordinates
	v_transfer_time transfer.transfer_time%TYPE;
	v_latitude transfer.latitude%TYPE;
	v_longitude transfer.longitude%TYPE;
	
	--Variablen für Summen
	v_sum_country_daily DECIMAL := 0;
	v_sum_daily DECIMAL := 0;
	v_sum_monthly DECIMAL := 0;

BEGIN
	SELECT blocked, daily_limit, monthly_limit, distance_per_hour_max INTO v_blocked, v_daily_limit, v_monthly_limit, v_distance_per_hour_max FROM card WHERE card_num = $1;
	SELECT disallowed, daily_limit INTO v_c_disallowed, v_c_daily_limit FROM country_specific WHERE country_code = $5;
	SELECT disallowed, daily_limit INTO v_cc_disallowed, v_cc_daily_limit FROM country_specific_per_card WHERE card_num = $1 AND country_code = $5;

	
	SELECT transfer_time, latitude, longitude INTO v_transfer_time, v_latitude, v_longitude FROM transfer WHERE card_num = $1 ORDER BY transfer_time, card_num DESC LIMIT 1;
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
					IF v_latitude > 0 THEN
						delta_lat = abs($3 - v_latitude);
						delta_long = abs($4 - v_longitude);
						distance = sqrt( power(delta_lat, 2) + power(delta_long, 2) );
						
						delta_time = EXTRACT(EPOCH FROM age( current_t, v_transfer_time));
						-- interval zu sekunden?
						IF ( distance / (delta_time * 60 * 60) <= v_distance_per_hour_max ) THEN
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
$$ LANGUAGE plpgsql;