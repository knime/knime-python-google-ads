import knime.extension as knext

country_target_type_query = """SELECT 
  geo_target_constant.name, 
  geo_target_constant.id, 
  geo_target_constant.target_type, 
  geo_target_constant.status, 
  geo_target_constant.parent_geo_target, 
  geo_target_constant.resource_name, 
  geo_target_constant.country_code, 
  geo_target_constant.canonical_name 
FROM geo_target_constant 
WHERE 
  geo_target_constant.country_code = '$$country_code$$' 
  AND geo_target_constant.status = 'ENABLED' 
  AND geo_target_constant.target_type = '$$target_type$$' """


def get_country_type_query(country_code, target_type):
    return country_target_type_query.replace("$$country_code$$", country_code).replace(
        "$$target_type$$", target_type
    )
