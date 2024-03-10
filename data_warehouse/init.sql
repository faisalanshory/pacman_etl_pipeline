CREATE TABLE IF NOT EXISTS sales_data(
    id SERIAL PRIMARY KEY,
    name TEXT,
    main_category TEXT,
    sub_category TEXT,
    image TEXT,
    link TEXT,
    ratings REAL,
    no_of_ratings REAL,
    discount_price REAL,
    actual_price REAL,
    currency TEXT
    created_at timestamp NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS marketing_data(
    id TEXT PRIMARY KEY,
    prices_amountMax REAL,
    prices_amountMin REAL,
    prices_availability TEXT,
    prices_condition TEXT,
    prices_currency TEXT,
    prices_dateSeen TEXT,
    prices_isSale INTEGER,
    prices_merchant TEXT,
    prices_shipping TEXT,
    prices_sourceURLs TEXT,
    asins TEXT,
    brand TEXT,
    categories TEXT,
    dateAdded TIMESTAMP,
    dateUpdated TIMESTAMP,
    imageURLs TEXT,
    keys TEXT,
    manufacturerNumber TEXT,
    name TEXT,
    primaryCategories TEXT,
    sourceURLs TEXT,
    upc TEXT,
    weight TEXT,
    weight_unit TEXT
    created_at timestamp NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS journal_disaster_id(
    id SERIAL PRIMARY KEY,
    journal TEXT,
    datePublished TIMESTAMP,
    publication TEXT,
    writers TEXT,
    abstract TEXT,
    link TEXT
    created_at timestamp NOT NULL DEFAULT NOW()
);