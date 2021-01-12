from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '17T3577XicHakB-BOE72ww61mQbrTyPRUs7tBjKa8ba8'
PEOPLE_RANGE = 'People!A:M'
VALUE_OPTION = "USER_ENTERED"

def get_credentials():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    return service.spreadsheets()


def get_current(sheets):
    result = sheets.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=PEOPLE_RANGE).execute()
    return result.get('values', [])[1:]


def upload_new(sheets, records):
    def upload(sheets, rows):
        body = {
            'values': rows
        }

        result = sheets.values().append(
            spreadsheetId=SPREADSHEET_ID, range=PEOPLE_RANGE,
            valueInputOption=VALUE_OPTION, body=body).execute()

        print('{0} cells appended.'.format(result.get('updates').get('updatedCells')))
        return result.get('updates').get('updatedCells'), len(rows)

    sheets_rows = []
    total_cells = 0
    total_rows = 0
    for record in records:
        row = []
        for k, v in record.items():
            row.append(v)
        sheets_rows.append(row)

        # after 200 records, batch the uploads
        if len(sheets_rows) == 200:
            cells, rows = upload(sheets, sheets_rows)
            total_cells += cells
            total_rows += rows
            sheets_rows = []
    cells, rows = upload(sheets, sheets_rows)
    total_cells += cells
    total_rows += rows

    return total_cells, total_rows





if __name__ == "__main__":
    cred = get_credentials()
    records = [{'a': 'a1', 'b': 'b1'}, {'a': 'a2', 'b': 'b2'}]
    upload_new(cred, records)