import gzip
import csv
import collections
import ural
import tqdm
from shorteners import SHORTENER_DOMAINS


def count_domains(in_file, out_file):
    "permet de compter le nombre de fois qu'apparait un nom de domaine dans le fichier csv donné en paramètre s'il est dans le bon format et renvoie \
        le résultat dans un autre fichier csv"
    domain_counter = collections.Counter()
    domain_url = collections.defaultdict(list)

    with gzip.open(in_file, 'rt') as file:
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

    with open(out_file, 'w') as result_file:
        file_writer = csv.writer(result_file)
        file_writer.writerow(['Domain', 'Count', 'URL'])
        for domain_dict in domain_counter.most_common():
            tmp = ""
            for url in domain_url[domain_dict[0]]:
                tmp += f"{url},"
            tmp = tmp[:-1]
            file_writer.writerow([domain_dict[0], domain_dict[1], tmp])
            tmp = ""


def reduce_by_total_url_size(in_file, out_file):
    "réduit le nombre de domaines par rapport à la taille totale des URL"
    with open(out_file, 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open(in_file) as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            size = len(row['URL'])
            if size < 150:
                with open(out_file, 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def reduce_by_url_size(in_file, out_file):
    "réduit le nombre de domaines par rapport à la taille de chaque URL"
    with open(out_file, 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open(in_file) as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            tmp = row['URL']
            list_url = tmp.split(',')
            verif = True
            for url in list_url:
                if len(url) > 50:
                    verif = False
            if verif:
                with open(out_file, 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def remove_shorteners(in_file, out_file):
    "supprime les noms de domaine déjà connus comme des shorteners"
    with open(out_file, 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open(in_file) as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            if row['Domain'] not in SHORTENER_DOMAINS:
                with open(out_file, 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def reduce_by_path(in_file, out_file):
    "réduit le nombre de domaines en fonction de la taille du chemin de l'URL"
    with open(out_file, 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['Domain', 'Count', 'URL'])
    with open(in_file) as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            tmp = row['URL']
            list_url = tmp.split(',')
            verif = True
            for url in list_url:
                path = ural.urlpathsplit(url)
                if len(''.join(path)) > 20:
                    verif = False
            if verif:
                with open(out_file, 'a') as output_file:
                    csv_writer = csv.writer(output_file)
                    csv_writer.writerow([row['Domain'],row['Count'],row['URL']])


def main():
    "boucle principale"
    # print("test")
    reduce_by_path('reduced_file_v3.csv', 'reduced_file_v4.csv')


if __name__ == '__main__':
    main()
