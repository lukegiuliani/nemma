import shutil
import requests
import urllib
from urllib.parse import urlparse
import os

import progressbar

from bs4 import BeautifulSoup
import wget 

head_download_folder = "assets"

# ensure our download folder exists
if not os.path.exists(head_download_folder):
  os.makedirs(head_download_folder)


def download_state(state):

	url_list = []

	final_destination_folder = head_download_folder + '/price_and_demand'
	if not os.path.exists(final_destination_folder):
	  os.makedirs(final_destination_folder)

	for year in range(1998, 2016):
		innerbar = progressbar.ProgressBar(redirect_stdout=True)
		for month in innerbar(range(12)):

			filename = "PRICE_AND_DEMAND_%s%02d_%s.csv" % (year, month + 1, state)

			dest_filename = final_destination_folder + "/" + filename

			if os.path.exists(dest_filename):
				print("Skipping %s, it already exists." % filename)

			else:
				print("Downloading %s to %s." % (filename, final_destination_folder))

				full_url = "https://www.aemo.com.au/aemo/data/nem/priceanddemand/" + filename

				response = requests.get(full_url, stream=True)
				with open(dest_filename, 'wb') as out_file:
				   shutil.copyfileobj(response.raw, out_file)
				del response

download_state("VIC1")