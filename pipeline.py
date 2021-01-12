import time
import schedule
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
    'twitter': ('Twitter Profile', 'URL', False),
    'instagram': ('Instagram Profile', 'URL', False),
    'linkedin': ('LinkedIn Profile', 'URL', False),
    'facebook': ('Facebook Profile', 'URL', False),
    # === Event Information ===
    'event': ('Event Name', 'String', True),
    'event_venue': ('Event Venue', 'String', False),
    'event_url': ('Event Url', 'URL', False),
}

COLUMNS = ['event', 'name', 'graduation_year', 'school', 'company', 'job_title', 'interest_field', 'description',
           'twitter', 'instagram', 'linkedin', 'facebook', 'event_venue', 'event_url']


def schedule_run():
    schedule.every().day.at("21:07").do(run)
    while True:
        schedule.run_pending()
        time.sleep(60)


def run():
    print('running import...')
    sheets_credential = handle_sheets.get_credentials()
    records = handle_sheets.get_current(sheets_credential)

    uids = {generate_uid_from_list(record) for record in records}
    scraped_records = scrape.scrape()

    duplicate_uids = [0]
    new_records = order_new_records(scraped_records)
    new_records = compare_records(uids, new_records, duplicate_uids)
    total_cells, total_records = handle_sheets.upload_new(sheets_credential, new_records)
    print('======================')
    print('Total Cells Updated: ', total_cells)
    print('Total Records Uploaded', total_records)
    print('Duplicate Records Found', duplicate_uids[0])
    print('======================')


def compare_records(uids, scraped_records, duplicate_uids):
    for record in scraped_records:
        uid = generate_uid_from_dict(record)
        if uid not in uids:
            uids.add(uid)
            yield record
        else:
            duplicate_uids[0] += 1


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
            ordered_record[column] = record.get(column, '')
        yield ordered_record


if __name__ == '__main__':
    run()