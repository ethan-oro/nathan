import time
import scrape
import handle_sheets
from collections import OrderedDict


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
    'social_media': ('Social Media Links', 'List', False),
    # === Event Information ===
    'event': ('Event Name', 'String', True),
    'event_venue': ('Event Venue', 'String', False),
    'event_url': ('Event Url', 'String', False),
}


COLUMNS = ['event', 'name', 'graduation_year', 'school', 'company', 'job_title', 'interest_field', 'description',
           'social_media', 'event_venue', 'event_url']


def run():
    sheets_credential = handle_sheets.get_credentials()
    records = handle_sheets.get_current(sheets_credential)

    uids = {generate_uid_from_list(record) for record in records}
    scraped_records = scrape.scrape()

    new_records = order_new_records(scraped_records)
    new_records = compare_records(uids, new_records)
    handle_sheets.upload_new(sheets_credential, new_records)


def compare_records(uids, scraped_records):
    duplicate_uids = []
    for record in scraped_records:
        uid = generate_uid_from_dict(record)
        if uid not in uids:
            yield record
        else:
            duplicate_uids.append(uid)


def generate_uid_from_list(record):
    key_fields = []
    for i, col in enumerate(COLUMNS):
        if SCHEMA[col][2]:
            key_fields.append(record[i])
    return hash(tuple(key_fields))


def generate_uid_from_dict(record):
    key_fields = []
    for k, v in record.items():
        if SCHEMA[k][2]:
            key_fields.append(v)
    return hash(tuple(key_fields))


def order_new_records(records):
    for record in records:
        ordered_record = OrderedDict()
        for column in COLUMNS:
            ordered_record[column] = record[column]
        yield ordered_record


if __name__ == '__main__':
    run()