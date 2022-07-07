import time

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup


def parse_character_urls(html):
    soup = BeautifulSoup(html, "html.parser")
    character_link_tags = soup.find_all("a", {"class": "explore__link"})
    character_paths = ["https://www.marvel.com" + tag['href'] for tag in character_link_tags]
    return character_paths


def get_character_urls():
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)

    url = "https://www.marvel.com/characters"
    driver.get(url)

    all_character_urls = list()

    while True:
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            driver.implicitly_wait(10)
            html = driver.page_source
            character_urls = parse_character_urls(html)
            all_character_urls = all_character_urls + character_urls
            next = driver.find_element(by=By.CSS_SELECTOR, value=".pagination__item-nav-next > span")
            ActionChains(driver).move_to_element(next)
            WebDriverWait(driver, 10).until(EC.visibility_of(next))
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(next))
            next.send_keys(Keys.RETURN)
            time.sleep(3)
        except Exception:
            break

    # de-dupe url list
    all_character_urls = list(dict.fromkeys(all_character_urls))

    character_urls_file = open("character_urls.txt", "w")
    for character_url in all_character_urls:
        character_urls_file.write(character_url + "\n")
    character_urls_file.close()


if __name__ == "__main__":
    get_character_urls()
