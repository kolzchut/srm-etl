import dataflows as DF
import re

from dataflows_airtable import load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from conf import settings


# Helper to normalize a possibly concatenated taxonomy string into individual tags
def _split_human_situations(value):
    if not value:
        return []
    # If multiple occurrences of the prefix exist, reliably extract all
    if value.count('human_situations:') > 1:
        parts = re.findall(r'human_situations:[A-Za-z0-9_:-]+', value)
        return parts
    # Otherwise split on common delimiters (comma / whitespace)
    tokens = re.split(r'[\s,]+', value.strip())
    tokens = [t for t in tokens if t]
    # If splitting produced multiple human_situations:* keep only those, else keep original
    hs_tokens = [t for t in tokens if t.startswith('human_situations:')]
    if hs_tokens:
        return hs_tokens
    return [value]


class Situations():

    def __init__(self):
        self._situations = None
        self._rid_map = None

    @property
    def situations(self):
        if self._situations is None:
            self._situations = DF.Flow(
                load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_SITUATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY)
            ).results()[0][0]
        return self._situations

    @property
    def rid_map(self):
        if self._rid_map is None:
            self._rid_map = dict(
                (r['id'], r[AIRTABLE_ID_FIELD])
                for r in self.situations
            )
        return self._rid_map

    def convert_situation_list(self, situations):
        ret = []
        for s in situations:
            if s in self.rid_map:
                raw_value = self.rid_map[s]
                tags = _split_human_situations(raw_value)
                ret.extend(tags)
            else:
                print('UNKOWN SITUATION', s)
        # Deduplicate while preserving order
        seen = set()
        deduped = []
        for t in ret:
            if t not in seen:
                seen.add(t)
                deduped.append(t)
        return deduped


    def situations_for_age_range(self, min_age, max_age):
        ret = []
        if min_age is None: min_age = 0
        if max_age is None: max_age = 120

        if min_age <= 54 and max_age >= 31:
            ret.append('adults')
        if min_age <= 30 and max_age >= 20:
            ret.append('young_adults')
        if min_age <= 19 and max_age >= 13:
            ret.append('teens')
        if min_age <= 1 and max_age >= 0:
            ret.append('infants')
        if min_age <= 12 and max_age >= 2:
            ret.append('children')
        if max_age >= 55:
            ret.append('seniors')
        if len(ret) == 6:
            ret = []
        return ['human_situations:age_group:{}'.format(s) for s in ret]


    def situations_for_clr_target_population(self, target_population):
        ret = []
        for t in target_population:
            for s in self.situations:
                if s['click_lerevacha_target_populations'] and t in s['click_lerevacha_target_populations']:
                    ret.append(s['id'])
        return ret
