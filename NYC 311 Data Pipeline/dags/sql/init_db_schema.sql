DROP TABLE IF EXISTS raw CASCADE;
DROP TABLE IF EXISTS staging CASCADE;
DROP TABLE IF EXISTS prod CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA prod;


-- Raw
CREATE UNLOGGED TABLE raw.service_request (
    unique_id text,
    created_date timestamp,
    closed_date timestamp,
    resolution_action_updated_date timestamp,
    resolution_description text,
    complaint_type text,
    descriptor text,
    status text,
    open_data_channel_type text,
    agency text,
    agency_name text,
    bbl text,
    incident_address text,
    city text,
    borough text,
    community_board text,
    incident_zip text,
    latitude float,
    longitude float
);

-- Staging
CREATE TABLE staging.dim_complaint_type (
    complaint_type_id serial PRIMARY KEY,
    complaint_type text,
    descriptor text
);

CREATE TABLE staging.dim_agency (
    agency_id serial PRIMARY KEY,
    agency text,
    agency_name text
);

CREATE TABLE staging.dim_status (
    status_id serial PRIMARY KEY,
    status text
);

CREATE TABLE staging.dim_submission_channel (
    submission_channel_id serial PRIMARY KEY,
    submission_channel text
);

CREATE TABLE staging.dim_location (
    bbl_id PRIMARY KEY,
    incident_address text,
    city text,
    borough text,
    community_board text,
    incident_zip text,
    latitude float,
    longitude float
);

CREATE TABLE staging.fact_service_request (
    service_request_id PRIMARY KEY,
    complaint_type_id int,
    bbl_id int,
    agency_id int,
    status_id int,
    submission_channel_id int,
    created_date timestamp,
    closed_date timestamp,
    resolution_action_updated_date timestamp,
    resolution_description text
);
