CREATE TABLE IF NOT EXISTS raw_file_metadata (
    file_key          TEXT PRIMARY KEY,          -- logical key, e.g. 'products'
    file_name         TEXT        NOT NULL,       -- original filename, e.g. 'ProductMaster.csv'
    file_path         TEXT        NOT NULL,       -- full container path to the file
    file_hash         TEXT,                       -- MD5 of file content at last load
    file_size_bytes   BIGINT,
    last_modified_at  TIMESTAMP WITH TIME ZONE,   -- OS mtime at last load
    last_loaded_at    TIMESTAMP WITH TIME ZONE,   -- when this pipeline last loaded the file
    load_count        INTEGER     DEFAULT 0       -- increments on each successful load
);
