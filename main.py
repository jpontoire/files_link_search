import gzip
import csv
import collections
import ural
import tqdm
import urllib.parse


def count_domains(in_file, out_file):
    "permet de compter le nombre de fois qu'apparait un nom de domaine dans le fichier csv donné en paramètre s'il est dans le bon format et renvoie \
        le résultat dans un autre fichier csv"
    domain_counter = collections.Counter()
    domain_url = collections.defaultdict(set)

    with gzip.open(in_file, 'rt') as file:
        csv_reader = csv.DictReader(file)
        for row in tqdm.tqdm(csv_reader):
            urls = row['links']
            url_list = urls.split('|')
            for url in url_list:
                domain_name = ural.get_domain_name(url)
                if domain_name != None:
                    domain_counter[domain_name] += 1
                    if len(domain_url[domain_name]) < 10:
                        domain_url[domain_name].add(url)

    with open(out_file, 'w') as result_file:
        file_writer = csv.writer(result_file)
        file_writer.writerow(['Domain', 'Count', 'URLs'])
        for domain_dict in domain_counter.most_common():
            tmp = ""
            for url in domain_url[domain_dict[0]]:
                tmp += f"{url} "
            tmp = tmp[:-1]
            file_writer.writerow([domain_dict[0], domain_dict[1], tmp])
            tmp = ""


def reduce_by_total_url_size(in_file, out_file):
    "réduit le nombre de domaines par rapport à la taille totale des URL"
    with open(in_file) as input_file, open(out_file, 'w') as output_file:
        csv_reader = csv.DictReader(input_file)
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_reader.fieldnames)
        csv_writer.writeheader()
        for row in csv_reader:
            size = len(row['URLs'])
            if size < 150:
                csv_writer.writerow(row)


def reduce_by_url_size(in_file, out_file):
    "réduit le nombre de domaines par rapport à la taille de chaque URL"
    with open(in_file) as input_file, open(out_file, 'w') as output_file:
        csv_reader = csv.DictReader(input_file)
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_reader.fieldnames)
        csv_writer.writeheader()
        for row in csv_reader:
            tmp = row['URLs']
            list_url = tmp.split(' ')
            verif = True
            for url in list_url:
                if len(url) > 50:
                    verif = False
            if verif:
                csv_writer.writerow(row)


def remove_shorteners(in_file, out_file):
    "supprime les noms de domaine déjà connus comme des shorteners"
    with open(in_file) as input_file, open(out_file, 'w') as output_file:
        csv_reader = csv.DictReader(input_file)
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_reader.fieldnames)
        csv_writer.writeheader()
        for row in csv_reader:
            tmp = row['URLs']
            list_url = tmp.split(' ')
            verif = True
            for url in list_url:
                if ural.is_shortened_url(url):
                    verif = False
            if verif:
                csv_writer.writerow(row)


def reduce_by_path(in_file, out_file):
    "réduit le nombre de domaines en fonction de la taille du chemin de l'URL"
    with open(in_file) as input_file, open(out_file, 'w') as output_file:
        csv_reader = csv.DictReader(input_file)
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_reader.fieldnames)
        csv_writer.writeheader()
        for row in csv_reader:
            tmp = row['URLs']
            list_url = tmp.split(' ')
            verif = True
            for url in list_url:
                path = urllib.parse.urlsplit(url).path
                if len(path) > 20 or len(path) <= 1:
                    verif = False
            if verif:
                csv_writer.writerow(row)


def reduce_by_nb_url(in_file, out_file):
    "supprime les noms de domaine qui n'ont qu'une seule URL"
    with open(in_file) as input_file, open(out_file, 'w') as output_file:
        csv_reader = csv.DictReader(input_file)
        csv_writer = csv.DictWriter(output_file, fieldnames=csv_reader.fieldnames)
        csv_writer.writeheader()
        for row in csv_reader:
            tmp = row['URLs']
            list_url = tmp.split(' ')
            if len(list_url) > 1:
                csv_writer.writerow(row)


def shorteners_list(in_file, out_file):
    "conserve les domaines suceptibles d'être des shorteners"
    domain_dict = collections.defaultdict(set)
    with open(in_file) as input_file, open(out_file, 'w+') as output_file:
        csv_in_reader = csv.DictReader(input_file)
        fieldnames = ['Domain', 'URLs']
        csv_out_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        csv_out_writer.writeheader()
        for row in csv_in_reader:
            nb_redirects = row['redirect_count']
            initial_domain = row['Domain']
            resolved_domain = ural.get_domain_name(row['resolved_url'])
            domain_dict[initial_domain].add((resolved_domain, nb_redirects, row['URLs']))
        for domain in domain_dict:
            domains_count = 0
            domains_list = []
            list_urls = ""
            bool_redirect = False
            for resolved_dom, redirects, urls in domain_dict[domain]:
                list_urls += f"{urls} "
                if resolved_dom not in domains_list:
                    domains_list.append(resolved_dom)
                    domains_count += 1
                if redirects:
                    if int(redirects) >= 1:
                        bool_redirect = True
            list_urls = list_urls[:-1]
            if bool_redirect and domains_count > 2:
                csv_out_writer.writerow({'Domain':domain, 'URLs':list_urls})


def shorteners_list_2(in_file, out_file):
    "conserve les domaines suceptibles d'être des shorteners"
    domain_dict = collections.defaultdict(set)
    with open(in_file) as input_file, open(out_file, 'w+') as output_file:
        csv_in_reader = csv.DictReader(input_file)
        fieldnames = ['Domain', 'URLs']
        csv_out_writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        csv_out_writer.writeheader()
        for row in csv_in_reader:
            nb_redirects = row['redirect_count']
            initial_domain = row['Domain']
            resolved_domain = ural.get_domain_name(row['resolved_url'])
            domain_dict[initial_domain].add((resolved_domain, nb_redirects, row['URLs']))
        for domain in domain_dict:
            domains_count = 0
            domains_list = []
            list_urls = ""
            bool_redirect = False
            for resolved_dom, redirects, urls in domain_dict[domain]:
                list_urls += f"{urls} "
                if resolved_dom:
                    stripped_f_host = ural.get_fingerprinted_hostname(resolved_dom, strip_suffix=True)
                    if stripped_f_host not in domains_list and stripped_f_host != ural.get_fingerprinted_hostname(domain, strip_suffix=True):
                        domains_list.append(stripped_f_host)
                        domains_count += 1
                if redirects:
                    if int(redirects) >= 1:
                        bool_redirect = True
            list_urls = list_urls[:-1]
            if bool_redirect and domains_count > 1:
                csv_out_writer.writerow({'Domain':domain, 'URLs':list_urls})


def main():
    "boucle principale"
    # print("test")
    # count_domains('fakenews_tweets.csv.gz', 'csv_domains_count_url.csv')
    shorteners_list_2('result_minet.csv', 'test_shorteners_immo.csv')


if __name__ == '__main__':
    main()
