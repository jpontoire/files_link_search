import gzip
import csv
import collections
import ural

domain_counter = collections.Counter()

with gzip.open('fakenews_tweets.csv.gz', 'rt') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        url = row['links']
        domain_name = ural.get_domain_name(url)
        if domain_name != None:
            domain_counter[domain_name] += 1
print(domain_counter)