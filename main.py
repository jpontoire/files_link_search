import gzip
import csv
import collections
import ural
import tqdm

domain_counter = collections.Counter()
domain_url = collections.defaultdict(list)

with gzip.open('fakenews_tweets.csv.gz', 'rt') as file:
    csv_reader = csv.DictReader(file)
    for row in tqdm.tqdm(csv_reader):
        urls = row['links']
        url_list = urls.split('|')
        for url in url_list:
            domain_name = ural.get_domain_name(url)
            if domain_name != None:
                domain_counter[domain_name] += 1
                if len(domain_url[domain_name]) < 3:
                    domain_url[domain_name].append(url)

with open('domain_fakenews_tweets_test.csv', 'w') as result_file:
    file_writer = csv.writer(result_file)
    file_writer.writerow(['Domain', 'Count', 'URL'])
    for domain_dict in domain_counter.most_common():
        tmp = ""
        for url in domain_url[domain_dict[0]]:
            tmp += f"{url},"
        tmp = tmp[:-1]
        file_writer.writerow([domain_dict[0], domain_dict[1], tmp])
        tmp = ""
