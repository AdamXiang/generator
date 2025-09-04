from collections import namedtuple
from datetime import datetime
from functools import partial

# check the file
file_name = "nyc_parking_tickets_extract.csv"

# use iterator __next__ method to retrieve first and second row 
with open(file_name) as f:
    column_headers = next(f).strip('\n').split(',')
    sample_data = next(f).strip('\n').split(',')

# modify the column then can use for namedtuple
column_names = [ header.replace(' ', '_').lower() for header in column_headers ]

# create the Ticket namedtuple
Ticket = namedtuple('Ticket', column_names)

# read the actual raw data without header
def read_data():
    with open(file_name) as f:
        next(f)
        yield from f

# cast data type
# convert string into integer
def parse_integer(value, *, default=None):
    try:
        return int(value)
    except ValueError:
        return default
    
# convert string into date
def parse_date(value, *, default=None):
    date_format = '%m/%d/%Y'

    try:
        return datetime.strptime(value, date_format).date()
    except ValueError:
        return default
    
def parse_string(value, *, default=None):
    # remove leading or trailing space
    try:
        cleaned_str = value.strip()

        # check if contains anything
        if not cleaned_str:
            return default
        else:
            return cleaned_str
    except ValueError:
        return default

# Given the column choose the function
column_parsers = (
    parse_integer,
    parse_string,
    lambda x: parse_string(x, default=''),  # need to be callable, so cannot use parse_string(..., default='')
    partial(parse_string, default=''),
    parse_date,
    parse_integer,
    partial(parse_string, default=''),
    parse_string,
    lambda x: parse_string(x, default='')
)

def parse_row(row, *, default=None):
    fields = row.strip('\n').split(',')

    parsed_data = [func(field) for func, field in zip(column_parsers, fields)]

    # if there any none, remove that row
    if all(item is not None for item in parsed_data):
        return Ticket(*parsed_data)

    return default

# check how many row we abandon
# for row in read_data():
#     parsed_row = parse_row(row)
#     if parsed_row is None:
#         print(list(zip(column_names, row.strip('\n').split(','))), end='\n\n')

def parsed_data():
    for row in read_data():
        parsed = parse_row(row)

        if parsed:
            yield parsed