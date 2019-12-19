# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.2
#   kernelspec:
#     display_name: default_ds_venv
#     language: python
#     name: default_ds_venv
# ---

import requests
import bs4
import json
import os 
import logging 

def get_login_info():
    logging.info(f"loading login info from {os.getcwd()}")
    path = os.path.join(os.path.expanduser("~"), "airflow/login_data.json")
    with open(path) as file:
        login_data = json.load(file)
    return login_data


def login(login_info):
    """
    return the client if successfully login, otherwise return None.
    """
    url = 'http://www.jintiankansha.me/account/signin'
    client = requests.session()
    # Retrieve the CSRF token first
    client.get(url)  # sets the cookie
    client.cookies
    xsrf = client.cookies.items()[1]
    login_info[xsrf[0]] = xsrf[1]
    r = client.post(url, data=login_info)
    
    if r.status_code == 200:
        return client
    else:
        return None 

# +
def get_page_url(column, page_num):
    result = f"http://www.jintiankansha.me/column/{column}?page={page_num}"
    return result

def check_if_original(article):
    """ simple rule to determine if the article is written by the blogger-trader or just ads.
    if image < 2.
    """
    num_img = len(article.find_all("img"))
    return num_img < 2 

def parse_article_html(page_resp):
    """ make an article's page html to article json."""
    article_url = page_resp.url
    
    article_page_soup = bs4.BeautifulSoup(page_resp.text, "lxml")
    
    title_html = article_page_soup.find_all("h1")[0]
    title_text = title_html.contents[0]
    
    date = article_page_soup.find_all("small", {'class': 'gray'})[0]
    date_text = date.contents[4].replace(" ", "").split("\n")[3][:10]
    
    article_content = article_page_soup.find_all("div", {'class': 'rich_media_content'})[0]
    article_text = article_content.get_text('\n')
    is_original = check_if_original(article_content) or '[原创]' in title_text
    
    return {
    'title': title_text,
    'date': date_text,
    'url': article_url,
    'is_original': is_original,
    'text': article_text
    
}


# +
def get_path_dict():
    user_path = os.path.expanduser('~')
    base_path = f"{user_path}/blogger_articles"
    return {
        'articles_json_path' : f"{base_path}/articles_json",
        'articles_text_path' : f"{base_path}/articles_text",     
    }


def save_article(article_json, path_dict):
    date, title = article_json['date'], article_json['title']
    json_path, text_path = path_dict['articles_json_path'], path_dict['articles_text_path']
    with open(f"{json_path}/{date}_{title}.json", "w", encoding='utf8') as file:
        json.dump(article_json, file, ensure_ascii=False)
    with open(f"{text_path}/{date}_{title}.txt", "w", encoding='utf8') as file:
        file.writelines(article_json['text']) 


# -

def crawl_column(client, column, max_page, path_dict):
    page_num = 1
    crawled_page = client.get(get_page_url(column, page_num))
    crawled_page_content = bs4.BeautifulSoup(crawled_page.text, "lxml")

    while len(crawled_page_content) > 0 and page_num <= max_page:
        articles = crawled_page_content.find_all("div", {"class":"cell item"})
        for article_html in articles:
            find_article = article_html.find_all("a")
            if not find_article:
                continue
            article_link = find_article[0].attrs['href']
            
            article_page = client.get(article_link)  
            article_json = parse_article_html(article_page)

            if article_json['is_original']:
                article_title = article_json['title']
                logging.info(f"saving article {article_title}")
                save_article(article_json, path_dict)

        page_num += 1
        crawled_page = client.get(get_page_url(column, page_num))
        crawled_page_content = bs4.BeautifulSoup(crawled_page.text, "lxml")


def crawl_blogger_articles(max_page):
    columns = ["mQJLCUg4xH", "4k0SK6QY1U"]
    
    login_info = get_login_info()
    client = login(login_info)
    path_dict = get_path_dict()
    
    for column in columns:
        crawl_column(client, column, max_page, path_dict)
