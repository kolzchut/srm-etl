from dotenv import load_dotenv

from .utils import EnvVarStrategy as s
from .utils import get_env

load_dotenv()

import csv

csv.field_size_limit(2*1024*1024)


CLICK_API = r'https://clickrevaha-app.molsa.gov.il/public/solr?rows=1000&fq[]=lang_code_s:he&facet.limit=2000&defType=edismax&fq[]=group_id_is:1&fq[]=type_i:1&facet.field[]=group_id_is&facet.field[]={!ex=Target_Population_A_ss,Target_Population_ss}Target_Population_A_ss&facet.field[]={!ex=Target_Population_A_ss,Target_Population_ss}Target_Population_ss&facet.field[]={!ex=Domin_ss}Domin_ss&q=*&start=0&group.ngroups=true&group.field=GroupFamilyName_s&facet.pivot={!ex=Target_Population_A_ss,Target_Population_ss}Target_Population_A_ss,Target_Population_ss&mm=50%&pf[]=FamilyName_t^30 Service_Purpose_t^22 Age_Minimum_i^10 Age_Maximum_i^10 Target_Population_A_t^14  Target_Population_t^14 Domin_t^8 Naming_Outputs_t^6&qf[]=FamilyName_t^15 Service_Purpose_t^11 Age_Minimum_i^5 Age_Maximum_i^5 Target_Population_A_t^7  Target_Population_t^7 Domin_t^4 Naming_Outputs_t^3 text^1&bq[]=(product_id_i:(498 OR 198 OR 484))^0.005&bq[]=(product_id_i:(612))^200&fq[]=distribution_channel_is:1&fq[]=-group_name_s:"כתובות"'
# https://clickrevaha-sys.molsa.gov.il/api/solr?rows=1000'

SHIL_API = 'https://www.gov.il/he/api/BureausApi/Index?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca'

GOV_DATA_PROXY = 'https://www.gov.il/he/api/DataGovProxy/GetDGResults'

GUIDESTAR_USERNAME = get_env('ETL_GUIDESTAR_USERNAME')
GUIDESTAR_PASSWORD = get_env('ETL_GUIDESTAR_PASSWORD')
GUIDESTAR_API = 'https://www.guidestar.org.il/services/apexrest/api'

GOVMAP_API_KEY = get_env('ETL_GOVMAP_API_KEY')
GOVMAP_AUTH = 'https://ags.govmap.gov.il/Api/Controllers/GovmapApi/Auth'
GOVMAP_REQUEST_ORIGIN = 'https://www.kolzchut.org.il'
GOVMAP_GEOCODE_API = 'https://ags.govmap.gov.il/Api/Controllers/GovmapApi/Geocode'

AIRTABLE_BASE ="appiJqrVjke9ppPaE" #  Replaced for testing purposes only get_env('ETL_AIRTABLE_BASE')
AIRTABLE_ALTERNATE_BASE = get_env('ETL_AIRTABLE_ALTERNATE_BASE')
AIRTABLE_DATAENTRY_BASE = get_env('ETL_AIRTABLE_DATAENTRY_BASE')
AIRTABLE_DATA_IMPORT_BASE = get_env('ETL_AIRTABLE_DATA_IMPORT_BASE')
AIRTABLE_STAGING_BASE = "appiJqrVjke9ppPaE" # Replaced for testing purposes only "appxUxbrRRoUiSB4P"

AIRTABLE_VIEW = 'Grid view'
AIRTABLE_LOCATION_TABLE = 'Locations'
AIRTABLE_ORGANIZATION_TABLE = 'Organizations'
AIRTABLE_SERVICE_TABLE = 'Services'
AIRTABLE_BRANCH_TABLE = 'Branches'
AIRTABLE_SERVICE_TABLE = 'Services'
AIRTABLE_RESPONSE_TABLE = 'Responses'
AIRTABLE_SITUATION_TABLE = 'Situations'
AIRTABLE_PRESETS_TABLE = 'Presets'
AIRTABLE_HOMEPAGE_TABLE = 'Homepage'
AIRTABLE_MANUAL_FIXES_TABLE = 'Manual Fixes'
AIRTABLE_STATS_TABLE = 'Stats'
AIRTABLE_CARDS_TABLE = 'Cards'
AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE = 'Guidestar Service Taxonomy Mapping'
AIRTABLE_TAXONOMY_MAPPING_SOPROC_TABLE = 'soproc-service-tagging'
AIRTABLE_TAXONOMY_MAPPING_CLICK_TABLE = 'Click Service Taxonomy Mapping'

AIRTABLE_API_KEY = get_env('DATAFLOWS_AIRTABLE_APIKEY')

MAPBOX_ACCESS_TOKEN = get_env('ETL_MAPBOX_ACCESS_TOKEN')
MAPBOX_LIST_TILESETS = 'https://api.mapbox.com/tilesets/v1/srm-kolzchut'
MAPBOX_UPLOAD_CREDENTIALS = 'https://api.mapbox.com/uploads/v1/srm-kolzchut/credentials'
MAPBOX_CREATE_UPLOAD = 'https://api.mapbox.com/uploads/v1/srm-kolzchut'
MAPBOX_UPLOAD_STATUS = 'https://api.mapbox.com/uploads/v1/srm-kolzchut/'

MAPBOX_TILESET_ID = get_env('ETL_MAPBOX_TILESET_ID', 'srm-kolzchut.point-data')
MAPBOX_TILESET_NAME = get_env('ETL_MAPBOX_TILESET_NAME', 'SRM Geo Data')
MAPBOX_TILESET_INACCURATE_ID = get_env('ETL_MAPBOX_TILESET_INACCURATE_ID', 'srm-kolzchut.point-data-inaccurate')
MAPBOX_TILESET_INACCURATE_NAME = get_env('ETL_MAPBOX_TILESET_INACCURATE_NAME', 'SRM Geo Data Inaccurate')

GOOGLE_MAPS_API_KEY = get_env('ETL_GOOGLE_MAPS_API_KEY')

OPENELIGIBILITY_YAML_URL = (
    'https://raw.githubusercontent.com/kolzchut/openeligibility/main/taxonomy.tx.yaml'
)

BUDGETKEY_DATABASE_URL = 'postgresql://readonly:readonly@data-next.obudget.org/budgetkey'

DATA_DUMP_DIR = 'data'

ENV_NAME = get_env('ENV_NAME')
ES_HOST = get_env('ES_HOST')
ES_PORT = int(get_env('ES_PORT'))
ES_HTTP_AUTH = get_env('ES_HTTP_AUTH', required=False)

CKAN_HOST = get_env('CKAN_HOST')
CKAN_API_KEY = get_env('CKAN_API_KEY')
CKAN_OWNER_ORG = get_env('CKAN_OWNER_ORG')

LOCATION_BOUNDS_SOURCE_URL='https://srm-staging.datacity.org.il/dataset/0386f511-bd1b-4931-8c3c-88c45272f642/resource/a5bad1e6-a40f-4ab5-b73e-56cd4947d8fa/download/place_bounds_he.zip'

EMAIL_NOTIFIER_SENDER_EMAIL = get_env('EMAIL_NOTIFIER_SENDER_EMAIL')
EMAIL_NOTIFIER_PASSWORD = get_env('EMAIL_NOTIFIER_PASSWORD')
EMAIL_NOTIFIER_RECIPIENT_LIST = get_env('EMAIL_NOTIFIER_RECIPIENT_LIST',[],strategy=s.ARRAY)
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

CHILDCARE_API_URL = "https://parents.education.gov.il/prhnet/Api/MeonotController/GetExcel?0=%%CURRENT_YEAR%%&1=0&2=0&3=0&4=0&5=0&csrt=6690085414305040436"
DAYCARE_API_URL = "https://data.gov.il/api/3/action/datastore_search?resource_id=0f67a263-d9f4-44d4-9816-c96e9dfbc7e5"
