# import pandas as pd
# data = pd.read_csv('fakenews_tweets.csv.gz', nrows=100, compression='gzip')
# for i in data['links']:
#     print(i)

import gzip
import csv
import collections
import ural

dico = collections.defaultdict(int)

with gzip.open('fakenews_tweets.csv.gz', 'rt') as file:
    text = csv.DictReader(file, delimiter=',')
    count = 0
    for row in text:
        url = row['links']
        domain = ural.get_domain_name(url)
        if domain != None:
            dico[domain] += 1
print(dico)