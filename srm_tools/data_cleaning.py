
STOPWORDS = ['עמותת ', 'העמותה ל']


def clean_org_name(name):
    if not name:
        return ''
    for x in (
        'בעמ',
        'בע״מ',
        "בע'מ",
        'ע״ר',
        'חל״צ',
        'ע"',
        'ע"ר',
        '()',
    ):
        name = name.replace(x, '')
        name = name.strip(',.() ')
        name = name.strip()
    for word in STOPWORDS:
        name = name.replace(word, '')
    name = name.strip(' -,\n\t')
    return name
