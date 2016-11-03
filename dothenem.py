import shutil
import requests
import urllib
from urllib.parse import urlparse
import os

from bs4 import BeautifulSoup
import wget 

nemweb_domain = "http://www.nemweb.com.au"
head_download_folder = "assets"

# ensure our download folder exists
if not os.path.exists(head_download_folder):
  os.makedirs(head_download_folder)


def download_path(nemweb_path, destination_folder):

	url_list = []

	r = requests.get(nemweb_path)
	soup = BeautifulSoup(r.text, "html.parser")

	final_destination_folder = head_download_folder + '/' + destination_folder
	if not os.path.exists(final_destination_folder):
	  os.makedirs(final_destination_folder)

	for link in soup.find_all('a'):
	    
		href = link.get('href')

		if href[-4:] == '.zip': # only download zips for now

			url_list.append(href)

			full_url = nemweb_domain + href

			dest_filename = final_destination_folder + "/" + href.split('/')[-1]

			print("Downloading %s to %s." % (href.split('/')[-1], final_destination_folder))

			response = requests.get(full_url, stream=True)
			with open(dest_filename, 'wb') as out_file:
			    shutil.copyfileobj(response.raw, out_file)
			del response

def download_current_and_archive(slug):
	download_path("http://www.nemweb.com.au/Reports/CURRENT/" + slug + "/", "Current/" + slug)
	download_path("http://www.nemweb.com.au/REPORTS/ARCHIVE/" + slug + "/", "Archive/" + slug)

#categories = ["Adjusted_Prices_Reports", "Public_Prices", "Yesterdays_Bids_Reports"]
#categories = ["HistDemand", "Market_Notice", "Next_Day_Offer_Energy", "SEVENDAYOUTLOOK_FULL", "Yesterdays_MNSPBids_Reports", "SupplyDemand"]

for cat in categories:
	download_current_and_archive(cat)