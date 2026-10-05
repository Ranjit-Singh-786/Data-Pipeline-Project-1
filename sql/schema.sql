CREATE TABLE IF NOT EXISTS insurance (
    age INT NOT NULL,
    sex VARCHAR(16) NOT NULL,
    bmi DECIMAL(10, 4) NOT NULL,
    children INT NOT NULL,
    smoker VARCHAR(8) NOT NULL,
    region VARCHAR(16) NOT NULL,
    charges DECIMAL(14, 6) NOT NULL
);
