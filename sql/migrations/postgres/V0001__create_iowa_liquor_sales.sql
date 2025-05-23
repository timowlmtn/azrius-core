drop table if exists lincoln_liquors.iowa_liquor_sales cascade;

CREATE TABLE lincoln_liquors.iowa_liquor_sales
(
    ID                    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    INVOICE_ITEM_NUMBER   VARCHAR(50)  NOT NULL,
    DATE                  DATE,
    STORE_NUMBER          INTEGER,
    STORE_NAME            VARCHAR(100),
    ADDRESS               VARCHAR(200),
    CITY                  VARCHAR(100),
    ZIP_CODE              VARCHAR(20),
    STORE_LOCATION        VARCHAR(200),
    COUNTY_NUMBER         INTEGER,
    COUNTY                VARCHAR(100),
    CATEGORY              VARCHAR(50),
    CATEGORY_NAME         VARCHAR(100),
    VENDOR_NUMBER         INTEGER,
    VENDOR_NAME           VARCHAR(100),
    ITEM_NUMBER           VARCHAR(50),
    ITEM_DESCRIPTION      VARCHAR(200),
    PACK                  VARCHAR(50),
    BOTTLE_VOLUME_ML      INTEGER,
    STATE_BOTTLE_COST     NUMERIC(10,2),
    STATE_BOTTLE_RETAIL   NUMERIC(10,2),
    BOTTLES_SOLD          INTEGER,
    SALE_DOLLARS          NUMERIC(10,2),
    VOLUME_SOLD_LITERS    NUMERIC(12,3),
    VOLUME_SOLD_GALLONS   NUMERIC(12,3),
    CREATED_TIMESTAMP     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY            VARCHAR       DEFAULT CURRENT_USER,
    UPDATED_BY            VARCHAR       DEFAULT CURRENT_USER
);
