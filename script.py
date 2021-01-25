# Скрипт для динамического скрапинга данных

import time

# Для скрапинга
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as Bs

# Для Selenium
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Для БД
import mysql.connector
from mysql.connector import Error

headers = {'User-Agent': UserAgent().chrome}
driver = webdriver.Chrome()  # Selenium


def get_all_urls():
    """ Получение первых 50 URL-адресов. """
    page_number = [i for i in range(0, 50, 10)]

    all_urls = []
    for page in page_number:
        time.sleep(30)  # Sleeping
        resp = requests.get(
            'https://www.yelp.com/search?find_desc=Vegan+Cafe&find_loc=San+Francisco%2C+CA&ns=1&start={}'.format(page),
            headers=headers)
        html = resp.content
        soup = Bs(html, 'html.parser')

        # Получаем ссылки со страниц, срез использован для удаления рекламных блоков.
        for link in soup.find_all('a', class_='link__09f24__1kwXV link-color--inherit__09f24'
                                              '__3PYlA link-size--inherit__09f24__2Uj95')[2:-1:]:
            all_urls.append('https://www.yelp.com' + link.get('href'))

    return all_urls


def scrap_data(urls):
    """ Скрапинг стр. с использованием BeautifulSoup, Selenium и Google API """
    result_data = []
    for url in urls:
        time.sleep(10)  # Sleeping
        wait = WebDriverWait(driver, 500)
        driver.get(url)
        # Ожидаем прогрузки элемента для скрапинга
        wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "css-0")))
        html = driver.page_source

        soup = Bs(html, 'html.parser')

        # Выбор дива для тэгов
        div_for_tags = soup.find_all('div', class_='photo-header-content__373c0__j8x16')
        span_for_tags = div_for_tags[0].find_all('span', class_='text__373c0__2Kxyz')
        tags_res = []
        for a in span_for_tags:
            a_for_tags = a.find('a')
            if a_for_tags:
                tags_res.append(a_for_tags.text)

        # Выбор главного дива для контактов
        div_for_contacts = soup.find_all('div', class_='css-0')
        if len(div_for_contacts) > 1:
            div_for_contacts = div_for_contacts[1]
        else:
            div_for_contacts = div_for_contacts[0]

        p_for_contacts = div_for_contacts.find_all('p', class_='text__373c0__2Kxyz'),

        try:
            # Полная информация о контактах
            phone = p_for_contacts[0][3].text
            address = p_for_contacts[0][4].text.replace('Get Directions', '')
            link = div_for_contacts.find('a', class_='link__373c0__1G70M').text

        except IndexError:
            try:
                # Информация без сайта
                phone = p_for_contacts[0][1].text
                address = p_for_contacts[0][3].text.replace('Get Directions', '')
                link = None
            except IndexError:
                try:
                    # Из доступной информации только адрес
                    phone = None
                    address = p_for_contacts[0][1].text.replace('Get Directions', '')
                    link = None
                except IndexError:
                    print('Упс... похоже структура сайта изменилась. Ссылка - {}'.format(url))
                    continue

        name = soup.h1.string

        # Сплит адреса для получения промежуточных значений
        split_address = address.split(' ')
        community = split_address[1]
        postcode = split_address[-2] + ' ' + split_address[-1]

        rating = soup.find('div', class_='i-stars__373c0__1T6rz')['aria-label'].split(' ')[0]

        # Запрос к Google API для получения координат и рейтинга заведений
        address_for_api = '{},{}'.format(name, address)

        r = requests.get('https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={}'
                         '&inputtype=textquery&fields=formatted_address,name,rating,geometry&'
                         'key=AIzaSyCJY5ZP9F48iCkaD1R34DY7F8xDWjcINY0'.format(address_for_api))
        result_from_api = r.json()

        location_lat = result_from_api['candidates'][0]['geometry']['location']['lat']
        location_lng = result_from_api['candidates'][0]['geometry']['location']['lng']
        rating_google = result_from_api['candidates'][0]['rating']

        result_data.append({
            'name': name,
            'phone': phone,
            'link': link,
            'tags': ', '.join(tags_res),
            'address': address,
            'community': community,
            'postcode': postcode,
            'location_lat': location_lat,
            'location_lng': location_lng,
            'rating': rating,
            'rating_google': rating_google

        })

    return result_data


def create_connection(host_name, user_name, user_password, db_name):
    """ Создание подключения к БД """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Успешный коннект")
    except Error as e:
        print(f"Ошибка коннекта '{e}'.")

    return connection


def execute_query(connection, query):
    """ Запрос к БД """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запрос успешен.")
    except Error as e:
        print(f"Ошибка запроса '{e}'.")


def main():
    all_urls = get_all_urls()
    result_data = scrap_data(all_urls)
    # Конект к БД
    connection = create_connection("localhost", "root", "admin", 'test')
    # Инсерт
    for obj in result_data:
        query = """
                INSERT INTO scrap_data(name,phone,link,tags,address,community,postcode,location_lat,location_lng,rating,rating_google)
                VALUES('{}','{}','{}','{}','{}','{}','{}',{},{},'{}',{});
            """.format(obj['name'], obj['phone'], obj['link'], obj['tags'], obj['address'], obj['community'],
                       obj['postcode'], obj['location_lat'], obj['location_lng'], obj['rating'], obj['rating_google'])
        execute_query(connection, query)


main()
