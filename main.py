import gzip
import csv
import collections
import ural
import tqdm
from shorteners import SHORTENER_DOMAINS


def make_csv():
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


def reduce_nb_domain_v1():
    "réduit le nombre de domaines par rapport à la taille totale des URL"
    with open('reduced_file_v1.csv', 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open('domain_fakenews_tweets.csv') as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            size = len(row['URL'])
            if size < 150:
                with open('reduced_file_v1.csv', 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def reduce_nb_domain_v2():
    "réduit le nombre de domaines par rapport à la taille de chaque URL"
    with open('reduced_file_v2.csv', 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open('domain_fakenews_tweets.csv') as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            tmp = row['URL']
            list_url = tmp.split(',')
            verif = True
            for url in list_url:
                if len(url) > 50:
                    verif = False
            if verif:
                with open('reduced_file_v2.csv', 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def remove_shorteners():
    "supprime les noms de domaine déjà connus comme des shorteners"
    with open('reduced_file_v3.csv', 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open('reduced_file_v2.csv') as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            if row['Domain'] not in SHORTENER_DOMAINS:
                with open('reduced_file_v3.csv', 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def main():
    # print("test")
    remove_shorteners()


if __name__ == '__main__':
    main()
