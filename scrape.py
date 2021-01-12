import requests
import lxml.etree
import itertools
from urllib.parse import urljoin, quote


START_URL = 'https://alumni.harvard.edu/programs-events/gnn/city-directory'
URL_ROOT = 'https://alumni.harvard.edu/'

INTEREST_EVENT_ROOT = '//div[@class="networking-events-directory"]//div[@class="networking-event-wrapper"]//div[@class="title"]'
LOCATION_EVENT_ROOT = '//div[@class="block block-views"]//div[@class="networking-event-wrapper"]//div[@class="title"]'
EVENT_VALUE_DICT = {
    'event_url': (['a'], '@href'),
    'event': (['a'], 'text()'),
    'event_venue':  (['span'], 'text()'),
}

DETAIL_ROOT = '//div[@class="view-content"]//div[@class="attendee-wrapper"]'
DETAIL_VALUE_DICT = {
    'name': (['div[@class="views-field views-field-title"]', 'span'], 'text()'),
    'graduation_year': (['div[@class="views-field views-field-field-attendee-class-year"]', 'div'], 'text()'),
    'school': (['div[@class="views-field views-field-field-attendee-school"]', 'div'], 'text()'),
    'company': (['div[@class="views-field views-field-field-attendee-company"]', 'div'], 'text()'),
    'job_title': (['div[@class="views-field views-field-field-attendee-company"]', 'div', 'em'], 'text()'),
    'interest_field': (['div[@class="views-field views-field-field-attendee-interest"]', 'div[@class="field-content"]'],
                       'text()'),
    'description': (['div[@class="views-field views-field-body"]', 'div', 'p'], 'text()'),
    'social_media': (['div[@class="views-field views-field-field-attendee-social-html"]', 'div'], '@href', 'a'),
}

SCHEMA = {
    # e.g. field_name : (Display Name, Type, Key Field)
    # === Individual Information ===
    'name': ('Name', 'String', True),
    'graduation_year': ('Graduation Year', 'String', True),
    'school': ('Harvard School', 'String', False),
    'company': ('Company', 'String', True),
    'interest_field': ('Field of Interest', 'String', False),
    'job_title': ('Job Title', 'String', False),
    'description': ('Description', 'String', False),
    'twitter': ('Twitter Profile', 'URL', False),
    'instagram': ('Instagram Profile', 'URL', False),
    'linkedin': ('LinkedIn Profile', 'URL', False),
    'facebook': ('Facebook Profile', 'URL', False),
    # === Event Information ===
    'event': ('Event Name', 'String', True),
    'event_venue': ('Event Venue', 'String', False),
    'event_url': ('Event Url', 'URL', False),
}

SOCIAL_MEDIA_TYPES = ['twitter', 'instagram', 'linkedin', 'facebook']

def scrape():
    main = requests.get(START_URL)
    tree = lxml.etree.HTML(main.text)

    for event in itertools.chain(
            scrape_xpath_dict_iter(tree, INTEREST_EVENT_ROOT, EVENT_VALUE_DICT),
            scrape_xpath_dict_iter(tree, LOCATION_EVENT_ROOT, EVENT_VALUE_DICT)):
        yield from post_process(scrape_detail(event))


def scrape_detail(base_record):
    base_record['event_url'] = urljoin(URL_ROOT, base_record['event_url'])
    detail = requests.get(base_record['event_url'])
    tree = lxml.etree.HTML(detail.text)
    for attendee in scrape_xpath_dict_iter(tree, DETAIL_ROOT, DETAIL_VALUE_DICT):
        yield {
            **base_record,
            **attendee,
        }


def post_process(records):
    def sort_social(row):
        for social_media_profile in row['social_media']:
            if isinstance(social_media_profile, list):
                social_media_profile = social_media_profile[0]
            social_media_profile = social_media_profile.lower()
            for social_media_type in SOCIAL_MEDIA_TYPES:
                if social_media_type in social_media_profile:
                    row[social_media_type] = social_media_profile
        del row['social_media']
        return row
    s0 = map(sort_social, records)
    return clean_text_iter(s0)


def row_from_vals(vals):
    rows = len(vals[next(iter(vals))])
    for row in range(rows):
        record = {}
        for col, l in vals.items():
            record[col] = l[row]
        yield record


def scrape_xpath_dict_iter(tree, root, value_dict, extra_dict={}):
    roots = tree.xpath(root)
    if extra_dict:
        value_dict.update(extra_dict)
    for root in roots:
        record = {}
        for name, xpath in value_dict.items():
            element = root
            for path in xpath[0]:
                element = element.find(path)
                if element is None:
                    break
            if element is None:
                record[name] = ''
            else:
                if len(xpath) == 3:
                    element = element.findall(xpath[2])
                    values = []
                    for e in element:
                        value = e.xpath(xpath[1])
                        values.append(value)
                    record[name] = values if len(values) > 1 else values[0]
                else:
                    values = element.xpath(xpath[1])
                    record[name] = values if not values or len(values) > 1 else values[0]
        yield record


def values_from_elements(elements):
    if isinstance(elements, str):
        return elements

    def convert_value(val):
        if isinstance(val, str):
            return str(val).strip()
        else:
            return ''.join(val.itertext())

    if isinstance(elements, list):
        if len(elements) == 0:
            return ''
        elif len(elements) == 1:
            return convert_value(elements[0])
        else:
            values_list = filter(lambda val: val != u'', map(convert_value, elements))
            if isinstance(values_list, list) and len(values_list) == 1:
                values_list = values_list[0]
            return list(values_list)


def clean_text_iter(records):
    for record in records:
        for k, v in record.items():
            if SCHEMA[k][1] == 'String':
                if isinstance(v, list):
                    v = ''.join(v)

                if v:
                    if v[0] == ',':
                        v = v[1:]
                record[k] = v.strip()
            if SCHEMA[k][1] == 'URL':
                if isinstance(v, list):
                    urls = []
                    for url in v:
                        if isinstance(url, list):
                            url = url[0]
                        urls.append(url.replace(' ', '%20'))
                    record[k] = ','.join(urls)
                else:
                    record[k] = v.replace(' ', '%20')
        yield record

if __name__ == '__main__':
    records = scrape()
    for record in records:
        print(record)
