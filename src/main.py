import re
import json

import requests
import bs4
from bs4 import BeautifulSoup


def response(url: str) -> requests.models.Response:
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    headers = {'User-Agent': user_agent}
    return requests.get(url, headers=headers)


def get_pages(response: requests.models.Response) -> list:
    PREFIX = 'https://www.datahackers.news'
    POSFIX = '?_data=routes%2Fp%2F%24slug'

    soup = BeautifulSoup(response.text, 'html.parser')
    contents = soup.find_all('div', {'class': 'space-y-3'})
    
    for content in contents:
        route = content.find('a').get('href')
        yield PREFIX + route + POSFIX


def get_page_data(response: requests.models.Response) -> dict:
    page_data = response.json()
    
    post = {
        'id': page_data['post']['id'],
        'title': page_data['post']['web_title'],
        'subtitle': page_data['post']['web_subtitle'],
        'created_at': page_data['post']['created_at'],
        'updated_at': page_data['post']['updated_at'],
        'url': page_data['requestUrl'],
    }

    post['data'] = parse_html(page_data)

    return post


def parse_html(page_data: dict) -> list:
    html = page_data['html']
    soup = BeautifulSoup(html, 'html.parser')
    contents = soup.find('div', {'id': 'content-blocks'})

    blocks = list()

    for id_, content in enumerate(contents):
        if content.name != 'style':
            if content.text != '':
                txt_content = content.text
                links = parse_links(content.find_all('a'))
            
                blocks.append({
                    'text': txt_content,
                    'links': links
                })
    
    return blocks


def parse_links(a_tags_content: bs4.element.Tag) -> list:
    pattern = r'^(.*?)(?=utm)'
    links = list()

    if not a_tags_content:
        return None

    for a_tag in a_tags_content:
        if a_tag:
            href_tag = a_tag.get('href')

            if 'utm' in href_tag:
                links.append(re.findall(pattern, href_tag)[0])
            else:
                links.append(href_tag)
    
    return links


if __name__ == '__main__':
    for page_n in list(range(1,33)):
        print(f'Página {page_n}.')

        url = f'https://www.datahackers.news/archive?page={page_n}'
        r = response(url)

        pages = get_pages(r)

        for p in pages:
            r = response(p)
            page_data = get_page_data(r)

            print(f'Salvando página {page_data["title"]}.')

            with open(f'src/outputs/{page_data["id"]}.json', 'w') as json_file:
                json.dump(page_data, json_file, ensure_ascii=False)
