import dataflows as DF

from dataflows_airtable import load_from_airtable, dump_to_airtable, AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.logger import logger

class ManualFixes():

    def __init__(self) -> None:
        self.manual_fixes = DF.Flow(
            load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_MANUAL_FIXES_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
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
            ).results()[0][0]
            logger.info(f'Got {len(var)} {table} records')
            var = dict((r[AIRTABLE_ID_FIELD], r) for r in var)
        return var

    def response_ids(self, slugs):
        self.responses = self.fetch_aux_table(self.responses, settings.AIRTABLE_RESPONSE_TABLE)
        return sorted(self.responses[k]['id'] for k in slugs)

    def situation_ids(self, slugs):
        self.situations = self.fetch_aux_table(self.situations, settings.AIRTABLE_SITUATION_TABLE)
        return sorted(self.situations[k]['id'] for k in slugs)

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
                    if field in row:
                        status = self.status.setdefault(fix_id, {
                            AIRTABLE_ID_FIELD: fix_id,
                            'etl_status': 'Obsolete'
                        })
                        self.used.add(fix_id)
                        actual_value = row.get(field)
                        if field == 'responses':
                            current_value = sorted(k.strip() for k in current_value.split(','))
                            actual_value = self.response_ids(actual_value)
                            fixed_value = sorted(k.strip() for k in fixed_value.split(','))
                        elif field == 'situations':
                            current_value = sorted(k.strip() for k in current_value.split(','))
                            actual_value = self.situation_ids(actual_value)
                            fixed_value = sorted(k.strip() for k in fixed_value.split(','))

                        if actual_value == current_value:
                            row[field] = fixed_value
                            status['etl_status'] = 'Active'

        return DF.Flow(
            func,
            DF.finalizer(self.finalize)
        )

    def finalize(self):
        records = [self.status[id] for id in self.used]
        if len(records) > 0:
            logger.info(f'Updating {len(records)} manual fix records')
            DF.Flow(
                records,
                DF.update_resource(-1, name='manual_fixes'),
                dump_to_airtable({
                    (settings.AIRTABLE_BASE, settings.AIRTABLE_MANUAL_FIXES_TABLE): {
                        'resource-name': 'manual_fixes',
                        'typecast': True
                    }
                }, settings.AIRTABLE_API_KEY),
            ).process()

    
