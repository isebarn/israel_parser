from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
import os
from bs4 import BeautifulSoup
import pandas
from datetime import datetime
import threading
import urllib3
import re

car_url = 'https://www.ad.co.il/{}?pageindex={}'

def save(data, category):
  #chunks = [data[i:i + 50000] for i in range(0, len(data), 50000)]

  #for idx, chunk in enumerate(chunks):
  df = pandas.DataFrame(data).to_excel("{}.xlsx".format(category.replace('/','-')))

def run(category, num_pages, category_function):
  driver = webdriver.Remote(os.environ.get('BROWSER'), DesiredCapabilities.FIREFOX)

  results = []
  if num_pages == 0:
    num_pages = 100000

  for page in range(1, num_pages):
    try:
      driver.get(car_url.format(category, page))

    except Exception as e:
      continue

    ads = driver.find_elements_by_xpath("//div[@data-modeltype]")

    if len(ads) == 0:
      break

    for ad in ads:
      result = basics(ad)
      result = category_function(result, ad)

      results.append(result)



  # remove duplicates
  ad_dict = {}
  for ad in results:
    ad_dict[ad['url']] = ad

  ads = []
  for k,v in ad_dict.items():
    ads.append(v)

  save(ads, category)

  driver.close()

def basics(ad):
    result = {}
    result["url"] = 'https://www.ad.co.il/ad/{}'.format(ad.get_attribute("data-oid"))
    result['price'] = ad.get_attribute("data-price")
    result['contact'] = ad.get_attribute("data-contact")
    result['area'] = ad.get_attribute('data-salearea')
    result['city'] = ad.get_attribute("data-city")


    result['created'] = datetime.fromtimestamp(int(str(ad.get_attribute('data-created'))[0:-3]))

    phone = result['phone1'] = ad.get_attribute("data-phone").split('/')
    result['phone1'] = phone[0].replace('-', '')
    result['phone2'] = phone[1].replace('-', '') if len(phone) == 2 else ''

    result['description'] = ad.get_attribute("data-desc").replace("\n", '')

    return result


def car(result, ad):
  result['maker'] = ad.get_attribute("data-man")
  result['model'] = ad.get_attribute("data-model")
  result['year'] = ad.get_attribute("data-year")
  result['driven'] = ad.get_attribute("data-km")

  return result

def nadlansale(result, ad):
  result['available'] = ad.get_attribute('data-enterdate')
  result['latitude'] = ad.get_attribute('data-lat')
  result['longitude'] = ad.get_attribute('data-lon')
  result['type'] = ad.get_attribute('data-saletype')
  result['size'] = ad.get_attribute('data-areasize')
  result['neighbourhood'] = ad.get_attribute('data-hood')

  return result

def nadlanrent(result, ad):
  result['address'] = ad.get_attribute('data-address')
  result['rooms'] = ad.get_attribute('data-rooms')
  result['size'] = ad.get_attribute('data-areasize')
  result['neighbourhood'] = ad.get_attribute('data-hood')
  result['num_payments_per_year'] = ad.get_attribute('data-yearpays')
  result['housepay'] = ad.get_attribute('data-housepay')
  result['enterdata'] = ad.get_attribute('data-enterdate')

  return result

def pets(result, ad):
  result['animal'] = ad.get_attribute('data-pettype')
  result['subtype'] = ad.get_attribute('data-petfamily')
  result['ad_type'] = ad.get_attribute('data-action')
  result['age'] = ad.get_attribute('data-age')
  result['gender'] = ad.get_attribute('data-gender')

  return result

def nadlanstudent(result, ad):
  result['address'] = ad.get_attribute('data-address')
  result['rooms'] = ad.get_attribute('data-rooms')
  result['size'] = ad.get_attribute('data-areasize')
  result['neighbourhood'] = ad.get_attribute('data-hood')
  result['num_payments_per_year'] = ad.get_attribute('data-yearpays')
  result['housepay'] = ad.get_attribute('data-housepay')
  result['enterdata'] = ad.get_attribute('data-enterdate')

  return result

def yad2(result, ad):
  result['category'] = ad.get_attribute('data-maincat')
  result['subcategory'] = ad.get_attribute('data-subcat')
  result['subsubcategory'] = ad.get_attribute('data-subsubcat')
  result['condition'] = ad.get_attribute('data-condition')
  result['manufacturer'] = ad.get_attribute('data-man')

  return result


if __name__ == "__main__":
  results = []
  http = urllib3.PoolManager()

  for x in range(1,10):
    print(x)
    r = http.request("GET", 'https://www.ad.co.il/car?pageindex=2')
    page = r.data
    soup = BeautifulSoup(page, features="lxml")
    divs = soup.findAll("div", {"data-modeltype": True})

    for div in divs:
      results.append(div['data-price'])

  print(results)

  '''
  print(1)
  car_thread = threading.Thread(target=run, args=('car', 0, car))
  nadlanrent_thread = threading.Thread(target=run, args=('nadlanrent', 0, nadlanrent))
  nadlanstudent_thread = threading.Thread(target=run, args=('nadlanstudent', 0, nadlanstudent))

  car_thread.start()
  nadlanrent_thread.start()
  nadlanstudent_thread.start()

  car_thread.join()
  nadlanrent_thread.join()
  nadlanstudent_thread.join()



  print(2)
  nadlansale_thread = threading.Thread(target=run, args=('nadlansale', 0, nadlansale))
  pets_thread = threading.Thread(target=run, args=('pets', 0, pets))
  yad2_thread = threading.Thread(target=run, args=('yad2', 0, yad2))

  nadlansale_thread.start()
  pets_thread.start()
  yad2_thread.start()

  nadlansale_thread.join()
  pets_thread.join()
  yad2_thread.join()


  print(3)
  archive_car_thread = threading.Thread(target=run, args=('archive/car', 0, car))
  archive_nadlanrent_thread = threading.Thread(target=run, args=('archive/nadlanrent', 0, nadlanrent))
  archive_nadlansale_thread = threading.Thread(target=run, args=('archive/nadlansale', 0, nadlansale))
  archive_pets_thread = threading.Thread(target=run, args=('archive/pets', 0, pets))

  archive_car_thread.start()
  archive_nadlanrent_thread.start()
  archive_nadlansale_thread.start()
  archive_pets_thread.start()

  archive_car_thread.join()
  archive_nadlanrent_thread.join()
  archive_nadlansale_thread.join()
  archive_pets_thread.join()

  print(4)

  yad2_archives = [18, 19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62]
  thread_chunks = [yad2_archives[i:i + 4] for i in range(0, len(yad2_archives), 4)]

  for idx, chunk in enumerate(thread_chunks):
    print("{}/{}".format(idx, len(thread_chunks)))
    threads = []

    for item in chunk:
      x = threading.Thread(target=run, args=('archive/c/{}'.format(item), 0, yad2))
      x.start()
      threads.append(x)

    for thread in threads:
      thread.join()
  '''