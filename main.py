import gzip
import csv
import collections
import ural

domain_counter = collections.Counter()
count = 0

with gzip.open('fakenews_tweets.csv.gz', 'rt') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        count+=1
        url = row['links']
        domain_name = ural.get_domain_name(url)
        if domain_name != None:
            domain_list = domain_name.split('|')
            for domain in domain_list:
                domain = domain.strip()
                domain_counter[domain] += 1
        if count == 100000:
            print(domain_counter)
            count = 0
print(domain_counter)