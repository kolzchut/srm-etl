from dotenv import load_dotenv

from .utils import EnvVarStrategy as s
from .utils import get_env

load_dotenv()


CLICK_API = 'https://clickrevaha-sys.molsa.gov.il/api/solr?rows=1000'

SHIL_API = 'https://www.gov.il/he/api/BureausApi/Index?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca'

GUIDESTAR_USERNAME = get_env('ETL_GUIDESTAR_USERNAME')
GUIDESTAR_PASSWORD = get_env('ETL_GUIDESTAR_PASSWORD')
GUIDESTAR_API = 'https://www.guidestar.org.il/services/apexrest/api'

GOVMAP_API_KEY = get_env('ETL_GOVMAP_API_KEY')
GOVMAP_AUTH = 'https://ags.govmap.gov.il/Api/Controllers/GovmapApi/Auth'
GOVMAP_REQUEST_ORIGIN = 'https://www.kolzchut.org.il'
GOVMAP_GEOCODE_API = 'https://ags.govmap.gov.il/Api/Controllers/GovmapApi/Geocode'

AIRTABLE_BASE = get_env('ETL_AIRTABLE_BASE')
AIRTABLE_VIEW = 'Grid view'
AIRTABLE_LOCATION_TABLE = 'Locations'
AIRTABLE_ORGANIZATION_TABLE = 'Organizations'
AIRTABLE_SERVICE_TABLE = 'Services'
AIRTABLE_BRANCH_TABLE = 'Branches'
AIRTABLE_SERVICE_TABLE = 'Services'
AIRTABLE_RESPONSE_TABLE = 'Responses'
AIRTABLE_SITUATION_TABLE = 'Situations'
AIRTABLE_API_KEY = get_env('DATAFLOWS_AIRTABLE_APIKEY')

MAPBOX_ACCESS_TOKEN = get_env('ETL_MAPBOX_ACCESS_TOKEN')
MAPBOX_LIST_TILESETS = 'https://api.mapbox.com/tilesets/v1/srm-kolzchut'
MAPBOX_UPLOAD_CREDENTIALS = 'https://api.mapbox.com/uploads/v1/srm-kolzchut/credentials'
MAPBOX_CREATE_UPLOAD = 'https://api.mapbox.com/uploads/v1/srm-kolzchut'
MAPBOX_UPLOAD_STATUS = 'https://api.mapbox.com/uploads/v1/srm-kolzchut/'

GOOGLE_MAPS_API_KEY = get_env('ETL_GOOGLE_MAPS_API_KEY')

OPENELIGIBILITY_YAML_URL = (
    'https://raw.githubusercontent.com/hasadna/openeligibility/main/taxonomy.tx.yaml'
)

BUDGETKEY_DATABASE_URL = 'postgresql://readonly:readonly@data-next.obudget.org/budgetkey'

DATA_DUMP_DIR = 'data'

ES_HOST = get_env('ES_HOST')
ES_PORT = int(get_env('ES_PORT'))
ES_HTTP_AUTH = get_env('ES_HTTP_AUTH', required=False)

CKAN_HOST = get_env('CKAN_HOST')
CKAN_API_KEY = get_env('CKAN_API_KEY')
CKAN_OWNER_ORG = get_env('CKAN_OWNER_ORG')
