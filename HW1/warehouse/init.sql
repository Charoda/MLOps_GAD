CREATE TABLE predictions (
    business_date date NOT NULL,
    model_version text NOT NULL,
    customer_id text NOT NULL,
    score numeric(10,8) NOT NULL CHECK (score BETWEEN 0 AND 1),
    label integer NOT NULL CHECK (label IN (0,1)),
    PRIMARY KEY (business_date, model_version, customer_id)
);
