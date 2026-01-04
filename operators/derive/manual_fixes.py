import dataflows as DF

from dataflows_airtable import load_from_airtable, dump_to_airtable, AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.logger import logger

class ManualFixes():

    def __init__(self) -> None:
        # Load manual fixes using the configured view first
        self._reloaded_manual_fixes = False
        self._load_manual_fixes(view=settings.AIRTABLE_VIEW)
        self.status = dict()
        self.used = set()
        self.responses = None
        self.situations = None

    def _load_manual_fixes(self, view=None):
        """Load manual fixes from Airtable. If view is None, load without applying a view filter."""
        try:
            self.manual_fixes = DF.Flow(
                load_from_airtable(
                    settings.AIRTABLE_DATA_IMPORT_BASE,
                    settings.AIRTABLE_MANUAL_FIXES_TABLE,
                    view,
                    settings.AIRTABLE_API_KEY,
                ),
            ).results()[0][0]
        except Exception as e:
            logger.error(
                'Failed loading Manual Fixes from base=%s table=%s view=%s: %s',
                settings.AIRTABLE_DATA_IMPORT_BASE,
                settings.AIRTABLE_MANUAL_FIXES_TABLE,
                view,
                e,
            )
            raise
        logger.info(
            'Got %d manual fix records from base=%s table=%s view=%s',
            len(self.manual_fixes),
            settings.AIRTABLE_DATA_IMPORT_BASE,
            settings.AIRTABLE_MANUAL_FIXES_TABLE,
            view,
        )
        # Key by Airtable record id
        self.manual_fixes = dict(
            (r[AIRTABLE_ID_FIELD], r) for r in self.manual_fixes
        )

    def fetch_aux_table(self, var, table):
        if var is None:
            var = DF.Flow(
                load_from_airtable(settings.AIRTABLE_BASE, table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
                DF.select_fields([AIRTABLE_ID_FIELD, 'id']),
            ).results(on_error=None)[0][0]
            logger.info(f'Got {len(var)} {table} records')
            var = dict((r['id'], r) for r in var)
        return var

    def normalize_ids(self, slugs):
        slugs = slugs or ''
        return ','.join(sorted(filter(None, set(s.strip() for s in slugs.split(',')))))

    def apply_manual_fixes(self):
        def func(row):
            manual_fixes = row.get('fixes')
            if manual_fixes is not None:
                for fix_id in manual_fixes:
                    if fix_id not in self.manual_fixes:
                        # One-time fallback: reload without view filter in case the view hides the record
                        if not getattr(self, '_reloaded_manual_fixes', False):
                            logger.warning(
                                'Manual fix %s not found in current cache (count=%d). Reloading Manual Fixes without view filter...',
                                fix_id,
                                len(self.manual_fixes),
                            )
                            self._load_manual_fixes(view=None)
                            self._reloaded_manual_fixes = True
                        # Check again after reload
                        if fix_id not in self.manual_fixes:
                            logger.error(
                                'Manual fix %s not found; referenced by record id=%s airtable_id=%s name=%s source=%s (base=%s table=%s). Loaded fixes=%d',
                                fix_id,
                                row.get('id'),
                                row.get(AIRTABLE_ID_FIELD),
                                row.get('name'),
                                row.get('source'),
                                settings.AIRTABLE_DATA_IMPORT_BASE,
                                settings.AIRTABLE_MANUAL_FIXES_TABLE,
                                len(self.manual_fixes),
                            )
                            raise AssertionError(
                                f"Manual fix {fix_id} not found (record id={row.get('id')} airtable_id={row.get(AIRTABLE_ID_FIELD)} name={row.get('name')})"
                            )
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
                        current_value = self.normalize_ids(current_value)
                        # extra_field = 'response_ids'
                        # extra_value = [x.strip() for x in fixed_value.split(',')]
                        fixed_value = self.normalize_ids(fixed_value)
                        actual_value = ','.join(sorted(actual_value or []))
                    elif field == 'situations':
                        current_value = self.normalize_ids(current_value)
                        # extra_field = 'situation_ids'
                        # extra_value = [x.strip() for x in fixed_value.split(',')]
                        fixed_value = self.normalize_ids(fixed_value)
                        actual_value = ','.join(sorted(actual_value or []))

                    if actual_value == current_value or current_value == '*':
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
        records = [self.status[id] for id in self.used]
        if len(records) > 0:
            logger.info(f'Updating {len(records)} manual fix records')
            batch_size = 50  # Adjust this number based on your data
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                DF.Flow(
                    batch,
                    DF.update_resource(-1, name='manual_fixes'),
                    dump_to_airtable({
                        (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_MANUAL_FIXES_TABLE): {
                            'resource-name': 'manual_fixes',
                            'typecast': True
                        }
                    }, settings.AIRTABLE_API_KEY),
                ).process()
