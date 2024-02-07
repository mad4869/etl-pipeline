import requests
import pandas as pd
from bs4 import BeautifulSoup


def extract_web():
    URL = "https://inet.detik.com/indeks"

    full_data = []

    for page in range(1, 251):
        resp = requests.get(f"{URL}/{page}")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")

            # get all the articles
            medias = soup.find_all("div", class_="media")

            for media in medias:
                # get the article's link
                link = media.find("a", class_="media__link")
                href = link.get("href")

                # filter out videos and pictures articles
                if "20.detik.com" not in href and "fotoinet" not in href:
                    content = requests.get(href)
                    content_soup = BeautifulSoup(content.text, "html.parser")
                    content_detail = content_soup.find("article", class_="detail")

                    if content_detail:
                        content_title = (
                            content_detail.find("h1", class_="detail__title").text
                            or None
                        )
                        content_author = (
                            content_detail.find("div", class_="detail__author").text
                            or None
                        )
                        content_date = (
                            content_detail.find("div", class_="detail__date").text
                            or None
                        )

                        content_body = (
                            content_detail.find("div", class_="detail__body").text
                            or None
                        )

                        content_data = {
                            "title": content_title,
                            "author": content_author,
                            "date": content_date,
                            "body": content_body,
                        }

                        full_data.append(content_data)

    df = pd.DataFrame(full_data)

    return df
