CREATE OR REPLACE PROCEDURE REFRESH_ICEBERG_METADATA_SP(
    ICEBERG_TABLE_NAME VARCHAR,
    EXTERNAL_VOLUME_BASE_URL VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // 1. Get the current metadata location as JSON string
    var get_info_sql = "SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('" + ICEBERG_TABLE_NAME + "')";
    var stmt = snowflake.createStatement({sqlText: get_info_sql});
    var result = stmt.execute();
    
    // Check if the query returned any row
    if (!result.next()) {
        return "ERROR: SYSTEM$GET_ICEBERG_TABLE_INFORMATION returned no result for table " + ICEBERG_TABLE_NAME;
    }

    var json_string = result.getColumnValue(1); 
    var parsed_json;
    
    try {
        // **FIX 1: Parse the JSON string into a JavaScript object**
        parsed_json = JSON.parse(json_string);
    } catch (e) {
        return "ERROR: Failed to parse JSON output from SYSTEM$GET_ICEBERG_TABLE_INFORMATION. Raw output: " + json_string;
    }
    
    // **FIX 2: Access the property correctly (Check for common casing)**
    var metadata_location = parsed_json.metadataLocation || parsed_json.METADATALOCATION; 
    
    if (!metadata_location) {
        return "ERROR: Could not find 'metadataLocation' in the JSON output.";
    }

    // 2. Extract the relative path
    var relative_path = '';
    
    var base_url = EXTERNAL_VOLUME_BASE_URL;
    if (!base_url.endsWith('/')) {
        base_url += '/';
    }

    // Line 22 (Corrected logic):
    if (metadata_location.startsWith(base_url)) { 
        // Remove the base URL part to get the relative path
        relative_path = metadata_location.substring(base_url.length);
    } else {
        return "ERROR: Metadata location (" + metadata_location + ") does not start with the provided base URL (" + base_url + ").";
    }

    // 3. Construct and execute the dynamic ALTER REFRESH command
    // ... (rest of the logic remains the same)
    
    var alter_refresh_sql = "ALTER ICEBERG TABLE " + ICEBERG_TABLE_NAME + " REFRESH '" + relative_path + "'";
    
    try {
        var refresh_stmt = snowflake.createStatement({sqlText: alter_refresh_sql});
        refresh_stmt.execute();
        return "SUCCESS: Table " + ICEBERG_TABLE_NAME + " refreshed to snapshot: " + relative_path;
    } catch (err) {
        return "ERROR: Refresh failed for table " + ICEBERG_TABLE_NAME + ". SQL: " + alter_refresh_sql + ". Error: " + err;
    }
$$;
