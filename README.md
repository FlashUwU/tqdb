# TQDB (Taggable Query Database)
> I've already stopped working on this project. This was made from an immature ideal when I still a rookie in programming, but it's interesting.

Every dataset has its own tag. The purpose of TQDB is making the querying job be more faster and less burden on RAM. Additionally, TQDB also performs well when handling backup works.

## Features
- Storing Tag Index with 250base numbers.
- Auto Commitment and Checking once connected to data_file.
- Can cope with sudden power outages
- Personalised Data Format.

## Installing
Run the following command in CMD to install:
```
pip install git+https://github.com/FlashUwU/tqdb.git
```

## Quick Start
1. Create a file for stroing data -> `your_file_name.data`
2. Run the following python codes:
```python
from tqdb import connect, DataContent

data_format = ("name", "pass")
tqdb = connect("your_file_name.data", data_format)

# Insert Data
tqdb.insert(DataContent(1, ("flash", "1qaz2wsx")))
tqdb.commit()

# Fetch Data
user = tqdb.fetch(1)
print(user) # output: {"name": b"flash", b"1qaz2wsx"}

# Replace Data
tqdb.replace(DataContent(1, ("flashuwu", "1234")))
tqdb.commit()
print(tqdb.fetch(1)) # output: {"name": b"flashuwu", b"1234"}

# Delete Data
tqdb.delete(1)
tqdb.commit()
```