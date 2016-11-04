# nemma

Python3 scripts to download and parse nem data into postgres.

# Setup

pip3 install -r requirements.txt

## `download-*.py`

Downloads assets based on 'category'. Downloads both archive and current files. It's dupe detection is pretty basic, it just checks if a file of the same name already exists based on the fact that NEM datestamp their data.

The price and demand content is presented (and stored?) a little differently on the aemo site, hence the different script. 

### Usage

For datafiles, edit the `categories` array at the bottom of the file, then run using `python3 getthenem.py`.

For price and demand, edit the date range. 

## `migrate-zips-to-postgres.py` / `load-price-and-demand.py`

Does what it says on the packet. Assumes you have downloaded stuff using download scripts.py. Gives some nice progress bars for fun. Note that committing the data currently takes 2x the extraction and loading :/.

Also note that for now it just trucates the database each run, so each time you are importing everything again 😮!

### Usage

Make sure you have a db called nem, and have set a config.cfg. Then just call the script.