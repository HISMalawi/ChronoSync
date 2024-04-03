def fetch_site_id(cursor):
    cursor.execute("SELECT property_value from global_property where property = 'current_health_center_id'")
    result = cursor.fetchone()
    print(result)
    # return the property value
    return result['property_value']