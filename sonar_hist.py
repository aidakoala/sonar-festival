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

# read data and examine first 10 rows
nodes = pd.read_csv(sys.argv[1])
# print(nodes.head(10))

plt.hist(nodes['location'], color = 'blue', edgecolor = 'black', bins = [1,2,3,4,5,6])
plt.title('Histogram of location popularity')
plt.xlabel('location')
plt.ylabel('nodes')
plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day2/location_popularity.png')
# plt.show()

# hours = map(get_hour, nodes['timestamp'])
# plt.hist(hours, color = 'blue', edgecolor = 'black', bins = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24])
# plt.title('Nodes distribution over a day')
# plt.xlabel('hour')
# plt.ylabel('nodes')
# plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day3/hist_nodes_hours.png')
# plt.show()


contacts = pd.read_csv(sys.argv[2])
# plt.hist(contacts['location'], color = 'blue', edgecolor = 'black', bins = [1,2,3,4,5,6])
# plt.title('Histogram of contacts per location')
# plt.xlabel('location')
# plt.ylabel('contacts')
# plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day3/contacts_per_location.png')
# plt.show()

# contacts_hours = map(get_hour_timestamp, contacts['tend'])
# print(contacts_hours)
# plt.hist(contacts_hours, color = 'blue', edgecolor = 'black', bins = 13)
# plt.title('Histogram of tend time')
# plt.xlabel('location')
# plt.ylabel('contacts')
# plt.show()

# average contact durations
contact_durations = map(contact_duration, contacts['tend'], contacts['tstart'])
print(max(contact_durations))
max_c = max(contact_durations)
count = 0
for i in contact_durations:
	if (i == max_c):
		print(count)
		break
	count = count + 1

# plt.hist(contact_durations, color = 'blue', edgecolor = 'black', bins = (615/25))
# plt.title('Histogram of average durtion of contacts')
# plt.xlabel('location')
# plt.ylabel('contacts')
# plt.savefig('/home/ghidusa/Documents/Disertation/Sonar Data/day2/avg_contact_duration.png')
# plt.show()
