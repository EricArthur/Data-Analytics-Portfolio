-- Populate Complaint Type Dimension

INSERT INTO staging.dim_complaint_type (complaint_type_id, complaint_type)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_complaint_type (complaint_type, descriptor)
SELECT  complaint_type,
        descriptor
FROM raw.service_request;


-- Populate Agency Dimension

INSERT INTO staging.dim_agency (agency_id, agency)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_agency (agency, agency_name)
SELECT  agency,
        agency_name
FROM raw.service_request;


-- Populate Status Dimension

INSERT INTO staging.dim_status (status_id, status)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_status (status)
SELECT  status
FROM raw.service_request;


-- Populate Submission Channel Dimension

INSERT INTO staging.dim_submission_channel (submission_channel_id, submission_channel)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_submission_channel (submission_channel)
SELECT submission_channel
FROM raw.service_request;


-- Populate Location Dimension

INSERT INTO staging.dim_location (bbl_id, incident_address)
SELECT -1, 'Unknown';

INSERT INTO staging.dim_location (bbl_id, incident_address, city, borough, community_board,
                                incident_zip, latitude, longitude)
SELECT  bbl_id,
        incident_address,
        city,
        borough,
        community_board,
        incident_zip,
        latitude,
        longitude
FROM raw.service_request;


-- Populate Service Request Fact Table

INSERT INTO staging.fact_service_request (service_request_id, complaint_type_id, bbl_id, agency_id,
                                        status_id, submission_channel_id, created_date, closed_date,
                                        resolution_action_updated_date, resolution_description)
SELECT  f.unique_id as service_request_id,
        COALESCE(c.complaint_type_id, -1) as complaint_type_id,
        COALESCE(l.bbl_id, -1) as bbl_id,
        COALESCE(a.agency_id, -1) as agency_id,
        COALESCE(s.status_id, -1) as status_id,
        COALESCE(sc.submission_channel_id, -1) as submission_channel_id,
        f.created_date,
        f.closed_date,
        f.resolution_action_updated_date,
        f.resolution_description
FROM raw.service_request f
LEFT JOIN staging.dim_complaint_type c ON f.complaint_type = c.complaint_type
LEFT JOIN staging.dim_location l ON f.bbl_id = l.bbl_id
LEFT JOIN staging.dim_agency a ON f.agency = a.agency
LEFT JOIN staging.dim_status s ON f.status = s.status
LEFT JOIN staging.submission_channel sc ON f.submission_channel = sc.submission_channel;


-- Migrate to Production Schema

INSERT INTO prod.dim_complaint_type
        (complaint_type_id, complaint_type, descriptor)
SELECT
        complaint_type_id, complaint_type, descriptor
FROM staging.dim_complaint_type;

INSERT INTO prod.dim_agency
        (agency_id, agency, agency_name)
SELECT
        agency_id, agency, agency_name
FROM staging.dim_agency;

INSERT INTO prod.dim_status
        (status_id, status)
SELECT
        status_id, status
FROM staging.dim_status;

INSERT INTO prod.dim_submission_channel
        (submission_channel_id, submission_channel)
SELECT
        submission_channel_id, submission_channel
FROM staging.dim_submission_channel;

INSERT INTO prod.dim_location
        (bbl_id, incident_address, city, borough, community_board, incident_zip, latitude, longitude)
SELECT
        bbl_id, incident_address, city, borough, community_board, incident_zip, latitude, longitude
FROM staging.dim_location;

INSERT INTO prod.fact_service_request
        (service_request_id, complaint_type_id, bbl_id, agency_id,
        status_id, submission_channel_id, created_date, closed_date,
        resolution_action_updated_date, resolution_description)
SELECT
        service_request_id, complaint_type_id, bbl_id, agency_id,
        status_id, submission_channel_id, created_date, closed_date,
        resolution_action_updated_date, resolution_description
FROM staging.fact_service_request;