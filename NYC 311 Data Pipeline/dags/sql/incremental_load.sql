-- Populate Complaint Type Dimension

INSERT INTO staging.dim_complaint_type (complaint_type, descriptor)
SELECT  complaint_type,
        descriptor
FROM raw.service_request
        ON CONFLICT (complaint_type) DO UPDATE SET
                descriptor = EXCLUDED.descriptor;


-- Populate Agency Dimension

INSERT INTO staging.dim_agency (agency, agency_name)
SELECT  agency,
        agency_name
FROM raw.service_request
        ON CONFLICT (agency) DO UPDATE SET
                agency_name = EXCLUDED.agency_name;


-- Populate Status Dimension

INSERT INTO staging.dim_status (status)
SELECT  status
FROM raw.service_request
        ON CONFLICT (status) DO UPDATE SET;


-- Populate Submission Channel Dimension

INSERT INTO staging.dim_submission_channel (submission_channel)
SELECT submission_channel
FROM raw.service_request
        ON CONFLICT (submission_channel) DO UPDATE SET;


-- Populate Location Dimension

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
FROM raw.service_request
        ON CONFLICT (bbl_id) DO UPDATE SET
                incident_address = EXCLUDED.incident_address,
                city = EXCLUDED.city,
                borough = EXCLUDED.borough,
                community_board = EXCLUDED.community_board,
                incident_zip = EXCLUDED.incident_zip,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude;

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


-- Merge Into Production Schema

INSERT INTO prod.dim_complaint_type
        (complaint_type_id, complaint_type, descriptor)
SELECT
        complaint_type_id, complaint_type, descriptor
FROM staging.dim_complaint_type
        ON CONFLICT(complaint_type_id) DO UPDATE SET
                complaint_type = EXCLUDED.complaint_type,
                descriptor = EXCLUDED.descriptor;

INSERT INTO prod.dim_agency
        (agency_id, agency, agency_name)
SELECT
        agency_id, agency, agency_name
FROM staging.dim_agency
        ON CONFLICT(agency_id) DO UPDATE SET
                agency = EXCLUDED.agency,
                agency_name = EXCLUDED.agency_name;

INSERT INTO prod.dim_status
        (status_id, status)
SELECT
        status_id, status
FROM staging.dim_status
        ON CONFLICT(status_id) DO UPDATE SET
                status = EXCLUDED.status;

INSERT INTO prod.dim_submission_channel
        (submission_channel_id, submission_channel)
SELECT
        submission_channel_id, submission_channel
FROM staging.dim_submission_channel
        ON CONFLICT(submission_channel_id) DO UPDATE SET
                submission_channel = EXCLUDED.submission_channel;

INSERT INTO prod.dim_location
        (bbl_id, incident_address, city, borough, community_board, incident_zip, latitude, longitude)
SELECT
        bbl_id, incident_address, city, borough, community_board, incident_zip, latitude, longitude
FROM staging.dim_location
        ON CONFLICT(bbl_id) DO UPDATE SET
                incident_address = EXCLUDED.incident_address,
                city = EXCLUDED.city,
                borough = EXCLUDED.borough,
                community_board = EXCLUDED.borough,
                incident_zip = EXCLUDED.incident_zip,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude;

INSERT INTO prod.fact_service_request
        (service_request_id, complaint_type_id, bbl_id, agency_id,
        status_id, submission_channel_id, created_date, closed_date,
        resolution_action_updated_date, resolution_description)
SELECT
        service_request_id, complaint_type_id, bbl_id, agency_id,
        status_id, submission_channel_id, created_date, closed_date,
        resolution_action_updated_date, resolution_description
FROM staging.fact_service_request
        ON CONFLICT(service_request_id) DO UPDATE SET
                complaint_type_id = EXCLUDED.complaint_type_id,
                bbl_id = EXCLUDED.bbl_id,
                agency_id = EXCLUDED.agency_id,
                status_id = EXCLUDED.status_id,
                submission_channel_id = EXCLUDED.submission_channel_id,
                created_date = EXCLUDED.created_date,
                closed_date = EXCLUDED.closed_date,
                resolution_action_updated_date = EXCLUDED.resolution_action_updated_date,
                resolution_description = EXCLUDED.resolution_description;