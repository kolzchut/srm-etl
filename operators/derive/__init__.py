from . import to_dp, to_es, to_mapbox, to_sql

if __name__ == '__main__':

    to_dp.srm_data_pull_flow().process()
    to_dp.flat_branches_flow().process()
    to_dp.flat_services_flow().process()
    to_dp.table_data_flow().process()
    to_sql.data_api_sql_flow().process()
    to_es.data_api_es_flow().process()
    to_mapbox.geo_data_flow().process()
    to_mapbox.push_mapbox_tileset()
