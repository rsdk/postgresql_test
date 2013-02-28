CREATE TABLE transfer (
    transfer_time TIMESTAMP,
    card_num BIGINT NOT NULL,
    amount DECIMAL NOT NULL,
    purpose VARCHAR(64),
    latitude FLOAT,
    longitude FLOAT,
    country_code VARCHAR(2),
    PRIMARY KEY (card_num, transfer_time)
);
CREATE TABLE card (
    card_num BIGINT NOT NULL,
    daily_limit DECIMAL,
    monthly_limit DECIMAL,
    blocked BOOLEAN,
    distance_per_hour_max SMALLINT,
    customer_name VARCHAR(64),
    PRIMARY KEY (card_num)
);
CREATE TABLE country_specific_per_card (
    card_num BIGINT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    disallowed BOOLEAN,
    daily_limit DECIMAL,
    PRIMARY KEY (card_num, country_code)
);
CREATE TABLE country_specific (
    country_code VARCHAR(2) NOT NULL,
    disallowed BOOLEAN,
    daily_limit DECIMAL,
    PRIMARY KEY (country_code)
);
CREATE TABLE countries (
    country_code VARCHAR(2) NOT NULL UNIQUE,
    country_name VARCHAR(64) NOT NULL,
    notes VARCHAR(200),
    PRIMARY KEY (country_code)
);
CREATE INDEX country_specific_per_card_idx ON country_specific_per_card (card_num, country_code);
CREATE INDEX transfer_idx ON transfer (card_num, country_code, transfer_time);
