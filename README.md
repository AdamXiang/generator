# Practice: Using Generators for General Tasks  

This project demonstrates how to use **Python generators** to handle general data processing tasks efficiently.  
The example uses the **NYC Parking Tickets dataset** to show how generators can be applied to:  

- Lazily read large files line by line without loading everything into memory.  
- Use `namedtuple` for structured data representation.  
- Apply parsing functions to convert raw string values into integers, dates, or cleaned strings.  
- Filter out invalid rows during parsing.  
- Count violations grouped by vehicle make using `defaultdict`.  

---

## 📂 Project Structure  
```bash
nyc_parking_tickets_extract.csv     # Example dataset
generator.py                        # Main Python script
```

---

## 🚀 How It Works  

1. **Read file header and sample row**  
   Uses the file iterator’s `__next__` method to retrieve the first two lines.  

2. **Prepare column names**  
   Column headers are normalized (lowercase, underscores) to build a `namedtuple`.  

3. **Define parsers**  
   - `parse_integer` → safely converts strings to integers.  
   - `parse_date` → parses dates into Python `date` objects.  
   - `parse_string` → strips whitespace, handles empty strings.  

4. **Row parsing**  
   Each row is parsed with the appropriate function (`column_parsers`) and converted into a `Ticket` namedtuple if valid.  

5. **Generator for parsed data**  
   `parsed_data()` yields one parsed record at a time, making the pipeline memory-efficient.  

6. **Aggregation**  
   `violation_count_by_make()` aggregates violations by vehicle make and returns a dictionary sorted by frequency.  

---

## 📝 Example Code  

```python
from collections import namedtuple, defaultdict
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
def parse_integer(value, *, default=None):
    try:
        return int(value)
    except ValueError:
        return default
    
def parse_date(value, *, default=None):
    date_format = '%m/%d/%Y'
    try:
        return datetime.strptime(value, date_format).date()
    except ValueError:
        return default
    
def parse_string(value, *, default=None):
    try:
        cleaned_str = value.strip()
        return cleaned_str if cleaned_str else default
    except ValueError:
        return default

# assign parsers per column
column_parsers = (
    parse_integer,
    parse_string,
    lambda x: parse_string(x, default=''),
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
    if all(item is not None for item in parsed_data):
        return Ticket(*parsed_data)
    return default

def parsed_data():
    for row in read_data():
        parsed = parse_row(row)
        if parsed:
            yield parsed

def violation_count_by_make(): 
    makes_counts = defaultdict(int)
    for data in parsed_data():
        makes_counts[data.vehicle_make] += 1
    return {
        mk: cnt
        for mk, cnt in sorted(makes_counts.items(), key=lambda tup: tup[1], reverse=True)
    }

print(violation_count_by_make())
```

## ✅ Key Takeaways

- Generators (yield) make the code memory-efficient for large datasets.

- Namedtuples improve readability and allow attribute-style access to fields.

- Parsing functions provide flexibility and robustness against dirty data.

- Combining generators with parsing and aggregation creates a clean ETL pipeline.
