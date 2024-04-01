import requests
from bs4 import BeautifulSoup
import re
import csv
import random

def parse(entries, gender):

    data = []

    regex = ".*\((\d+).*\)"
    for entry in entries:
        text = entry.text.strip()
        result = re.search(regex, text)
        if result:
            age = result.group(1)
            index = result.start(1)
            name = text[:index-1].strip()
            data.append([gender, name, age])
            #print(f'Gender: {gender}, Name: {name}, Age: {age}')
        else:
            data.append([gender, text])
            #print(f'Gender: {gender}, Name: {text}')
    
    return data

def write_to_csv(header, data):
    with open('martyrs.csv', 'w', encoding='UTF8', newline = '') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)


if __name__ == "__main__":

    url = 'https://interactive.aljazeera.com/aje/2024/israel-war-on-gaza-10000-children-killed/'

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # find <p> with class male/female

        male_child_entries = soup.find_all('p', class_='male')
        female_child_entries = soup.find_all('p', class_='female')

        male_data = parse(male_child_entries, "Male")
        female_data = parse(female_child_entries, "Female")

        header = ['gender', 'name', 'age']

        data = male_data + female_data

        random.shuffle(data)

        write_to_csv(header, data)