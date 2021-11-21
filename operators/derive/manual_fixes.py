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
        print(self.manual_fixes)
        self.status = dict()
        self.used = set()

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

    
