CREATE TABLE IF NOT EXISTS order_metrics(
    id BIGINT NOT NULL PRIMARY KEY,
    groupType VARCHAR(100) NOT NULL,
    groupKey VARCHAR(100) NOT NULL,
    orderCount BIGINT NOT NULL,
    totalQuantity BIGINT NOT NULL,
    totalRevenue DOUBLE NOT NULL,
    windowStart BIGINT NOT NULL,
    windowEnd BIGINT NOT NULL,
    consumedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS product_metrics(
    id BIGINT NOT NULL PRIMARY KEY,
    groupType VARCHAR(100) NOT NULL,
    groupKey VARCHAR(100) NOT NULL,
    orderCount BIGINT NOT NULL,
    totalQuantity BIGINT NOT NULL,
    totalRevenue DOUBLE NOT NULL,
    product VARCHAR(250) NOT NULL,
    category VARCHAR(250) NOT NULL,
    windowStart BIGINT NOT NULL,
    windowEnd BIGINT NOT NULL,
    consumedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS user_metrics(
    id BIGINT NOT NULL PRIMARY KEY,
    groupType VARCHAR(100) NOT NULL,
    groupKey VARCHAR(100) NOT NULL,
    orderCount BIGINT NOT NULL,
    totalQuantity BIGINT NOT NULL,
    totalRevenue DOUBLE NOT NULL,
    windowStart BIGINT NOT NULL,
    windowEnd BIGINT NOT NULL,
    consumedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dead_letter(
    id BIGINT PRIMARY KEY NOT NULL,
    message TEXT NOT NULL,
    error TEXT NOT NULL,
    rejectedAt BIGINT NOT NULL,
    consumedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);