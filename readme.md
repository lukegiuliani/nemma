# nemma

Python3 scripts to download and parse nem data into postgres.

# Setup

pip3 install -r requirements.txt

## download-nem-datafiles.py

Downloads assets based on 'category'. Downloads both archive and current files. It's dupe detection is pretty basic, it just checks if a file of the same name already exists based on the fact that NEM datestamp their data.

### Usage

Edit the `categories` array at the bottom of the file, then run using `python3 getthenem.py`

## migrate-zips-to-postgres.py

Does what it says on the packet. Assumes you have downloaded stuff using dothenem.py. Gives some nice progress bars for fun. Note that committing the data currently takes 2x the extraction and loading :/.

Also note that for now it just trucates the database each run, so each time you are importing everything again :-o!

### Usage

Make sure you have a db called nem. 

```
python3 migrate-zips-to-postgres.py
```

