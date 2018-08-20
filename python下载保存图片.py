#-*- coding:utf-8 -*-
import  os
import urllib2

img_url = "http://www.zhaolibin.com/test.jpg"
img_name  = 'new.jpg'
film_path = "/opt/openfea/workspace/fangongheike/"

def save_img(img_url,img_name,film_path):
    if not os.path.exists(film_path):
        os.makedirs(film_path)
    try :
        url = urllib2.Request(img_url)
        response = urllib2.urlopen(url,timeout=3)
        f = file(film_path + img_name, 'wb')
        f.write(response.read())
        f.close()
    except Exception,e:
        print e, response.read()

save_img(img_url,img_name,film_path)