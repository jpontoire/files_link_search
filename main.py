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
            domain_list = domain_name.split('|')
            for domain in domain_list:
                domain = domain.strip()
                domain_counter[domain] += 1
print(domain_counter.most_common(100))

result_file = open('domain_fakenews_tweets.csv', 'w')
file_writer = csv.writer(result_file)
for domain_dict in domain_counter.most_common():
    file_writer.writerow([domain_dict[0], domain_dict[1]])