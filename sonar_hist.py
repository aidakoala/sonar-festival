import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import time
import math
import sys

def get_hour(my_date):
	return datetime.strptime(my_date, "%Y-%m-%d %H:%M:%S").hour

def get_hour_timestamp(my_timestamp):
	return datetime.fromtimestamp(my_timestamp).hour

def contact_duration(tend, tstart):
	return math.floor((tend- tstart) / 60)

def hist_location_popularity(nodes, day):
	plt.hist(nodes['location'], color = 'blue', edgecolor = 'black', bins = [1,2,3,4,5,6,7])
	plt.title('Histogram of location popularity')
	plt.xlabel('location')
	plt.ylabel('nodes')
	plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day' + day + '/location_popularity.png')
	# plt.show()

def hist_nodes_hours(nodes, day, file_name):
	hours = map(get_hour_timestamp, nodes['tstart'])
	plt.hist(hours, color = 'blue', edgecolor = 'black', bins = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24])
	plt.title('Nodes distribution over a day')
	plt.xlabel('hour')
	plt.ylabel('nodes')
	plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day' + day + '/' + file_name)
	# plt.show()

def hist_contacts_per_location(contacts, day):
	plt.hist(contacts['location'], color = 'blue', edgecolor = 'black', bins = [1,2,3,4,5,6,7])
	plt.title('Histogram of contacts per location')
	plt.xlabel('location')
	plt.ylabel('contacts')
	plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day' + day + '/contacts_per_location.png')
	# plt.show()

def hist_average_contact_duration(contacts, day):
# average contact durations
	contact_durations = map(contact_duration, contacts['tend'], contacts['tstart'])
	max_c = max(contact_durations)
	print(max_c)
	count = 0
	for i in contact_durations:
		if (i == max_c):
			print(count)
			break
		count = count + 1

	bin_int = int(math.floor(max_c/20))
	print(bin_int)
	plt.hist(contact_durations, color = 'blue', edgecolor = 'black', bins = bin_int)
	plt.title('Histogram of average durtion of contacts')
	plt.xlabel('location')
	plt.ylabel('contacts')
	plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day' + day + '/avg_contact_duration_hour_16.png')
	# plt.show()



def main():
	# read data and examine first 10 rows
	# nodes = pd.read_csv(sys.argv[1])
	# print(nodes.head(10))
	# hist_location_popularity(nodes, sys.argv[3])
	# file_name = 'hist_nodes_hour_loc_' + sys.argv[4] + '.png'
	# location = sys.argv[4]
	# is_loc = nodes['location']==6
	# nodes_loc = nodes[is_loc]
	# hist_nodes_hours(nodes_loc, sys.argv[3], file_name)

	contacts = pd.read_csv(sys.argv[2])
	# hist_contacts_per_location(contacts, sys.argv[3])
	file_name = 'contacts_loc_hour_' + sys.argv[4] + '.png'
	location = sys.argv[4]
	filter_var = (contacts['tstart']==1434632400) & (contacts['location']==3)
	contacts_hour = contacts[filter_var]
	hist_average_contact_duration(contacts_hour, sys.argv[3])

main()