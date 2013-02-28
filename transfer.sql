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