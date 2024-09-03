import dataflows as DF

from dataflows_airtable import load_from_airtable, dump_to_airtable, AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.logger import logger

class ManualFixes():

    def __init__(self) -> None:
        self.manual_fixes = DF.Flow(
            load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_MANUAL_FIXES_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        ).results()[0][0]
        logger.info(f'Got {len(self.manual_fixes)} manual fix records')
        self.manual_fixes = dict(
            (r[AIRTABLE_ID_FIELD], r) for r in self.manual_fixes
        )
        self.status = dict()
        self.used = set()
        self.responses = None
        self.situations = None

    def fetch_aux_table(self, var, table):
        if var is None:
            var = DF.Flow(
                load_from_airtable(settings.AIRTABLE_BASE, table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
                DF.select_fields([AIRTABLE_ID_FIELD, 'id']),
            ).results(on_error=None)[0][0]
            logger.info(f'Got {len(var)} {table} records')
            var = dict((r['id'], r) for r in var)
        return var

    def response_ids(self, slugs):
        slugs = slugs or ''
        slugs = [s.strip() for s in slugs.split(',')]
        self.responses = self.fetch_aux_table(self.responses, settings.AIRTABLE_RESPONSE_TABLE)
        return sorted(self.responses[k][AIRTABLE_ID_FIELD] for k in slugs if k in self.responses)

    def situation_ids(self, slugs):
        slugs = slugs or ''
        slugs = [s.strip() for s in slugs.split(',')]
        self.situations = self.fetch_aux_table(self.situations, settings.AIRTABLE_SITUATION_TABLE)
        return sorted(self.situations[k][AIRTABLE_ID_FIELD] for k in slugs if k in self.situations)

    def apply_manual_fixes(self):
        def func(row):
            manual_fixes = row.get('fixes')
            if manual_fixes is not None:
                for fix_id in manual_fixes:
                    assert fix_id in self.manual_fixes, f'Manual fix {fix_id} not found'
                    fix = self.manual_fixes[fix_id]
                    field = fix['field']
                    current_value = fix['current_value']
                    fixed_value = fix['fixed_value']
                    self.status.setdefault(fix_id, {
                        AIRTABLE_ID_FIELD: fix_id,
                        'etl_status': 'Obsolete'
                    })
                    status = self.status[fix_id]
                    self.used.add(fix_id)
                    actual_value = row.get(field)
                    extra_field = None
                    extra_value = None
                    if field == 'responses':
                        current_value = self.response_ids(current_value)
                        # extra_field = 'response_ids'
                        # extra_value = [x.strip() for x in fixed_value.split(',')]
                        fixed_value = self.response_ids(fixed_value)
                        actual_value = sorted(actual_value or [])
                    elif field == 'situations':
                        current_value = self.situation_ids(current_value)
                        # extra_field = 'situation_ids'
                        # extra_value = [x.strip() for x in fixed_value.split(',')]
                        fixed_value = self.situation_ids(fixed_value)
                        actual_value = sorted(actual_value or [])

                    if actual_value == current_value:
                        row[field] = fixed_value
                        print('FIXED!', fix_id, field, str(actual_value)[:100], '->', str(fixed_value)[:100])
                        # if extra_field is not None:
                        #     row[extra_field] = extra_value
                        #     print('FIXED EXTRA!', fix_id, extra_field, actual_value, '->', extra_value)
                        status['etl_status'] = 'Active'
                    else:
                        print('NOT FIXED!', fix_id, field, str(actual_value)[:100], '!=', str(current_value)[:100])

        return DF.Flow(
            func,
        )

    def finalize(self):
        print('FINALIZING', self.used, self.status)
        records = [self.status[id] for id in self.used]
        if len(records) > 0:
            logger.info(f'Updating {len(records)} manual fix records')
            DF.Flow(
                records,
                DF.update_resource(-1, name='manual_fixes'),
                dump_to_airtable({
                    (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_MANUAL_FIXES_TABLE): {
                        'resource-name': 'manual_fixes',
                        'typecast': True
                    }
                }, settings.AIRTABLE_API_KEY),
            ).process()

    
