from lxml import html
import MySQLdb
from MySQLdb import connect
import requests
import re
import string
import sys


URL = 'https://www.imdb.com/name/nm000000'

#https://www.imdb.com/name/nm000000635/

#https://www.imdb.com/name/nm000000647/
#https://www.imdb.com/name/nm000000735
#https://www.imdb.com/name/nm000000785
#https://www.imdb.com/name/nm000000790

def initDBConnection(host, user, pw, dbname):
    return connect(host, user, pw, dbname)

def getActor(htmlTree):
    #td = htmlTree.xpath('//td[@id="overview-top"]/*/span[@class="itemprop"]')
    td = htmlTree.xpath('//td[@id="overview-top"]//span[@class="itemprop"]')
    #span = td.xpath('.//span[@class="itemprop"]')
    #print(span[0].text)
    #sys.exit(0)
    #td = htmlTree.xpath('//td[@id="overview-top"]/*/span')
    #print("actors name is: "+td[0].text)
    return td[0].text


def getMovieTitles(htmlTree):
    divs = htmlTree.xpath('//div[starts-with(@id, "actor-tt") or starts-with(@id, "actress-tt")]')
    movie_titles = []
    print(type(divs))
    print("div is: "+str(divs))
    if not divs:
        return False
    print(html.tostring(divs[0]))

    for d in divs:
        print(type(d))
        #print("i is: "+str(htmlstring(i)))
        movie_titles.append(d.xpath('.//b/a')[0].text)
        #print("i extracted is: "+str(tostring(divs[0].xpath('.//a'))))

    print("this is type of movie_titles: "+str(type(movie_titles)))
    print("and this is type of movie_titles[0]: "+str(type(movie_titles[0])))
    print("length of movie_titles is: "+str(len(movie_titles)))

    #for t in movie_titles: print("this is title: "+str(t[0].text))
    


    return movie_titles

orig_url = URL

for i in range(0,100000):

    URL = orig_url+str(i+1)

    print(URL)
    SystemExit

    r = requests.get(URL)
    tree = html.fromstring(r.content)

    print("type of tree is: "+str(type(tree)))

    mt = getMovieTitles(tree)

    if mt is False:
        continue

    for t in mt: print("movie title: "+t)

    db = initDBConnection("127.0.0.1", "root", "", "bacon")

    cursor = db.cursor()

    #cursor.execute("SELECT * FROM actor")

    cursor.execute("SHOW TABLES FROM bacon")

    print("content of actor table: "+str(cursor.fetchall()))

    actor_name = getActor(tree)

    print("actors name is: "+actor_name)

    cursor.execute("""SELECT actorID FROM actor WHERE name=%s""", (actor_name,))
    res = cursor.fetchone()

    last_actor_row_id = None

    print(res)

    if res is None:
        print("actor with name '"+actor_name+"' is not yet in database. adding.")
        cursor.execute("""INSERT INTO actor (name) VALUES (%s)""", (actor_name,))
        db.commit()
        last_actor_row_id = cursor.lastrowid
        print("actor last row id is: "+str(last_actor_row_id))
    else:
        print("actor with name '"+actor_name+"' is already in database")
        continue

    count = 0

    last_movie_row_id = None

    for m in mt:
        count += 1
        cursor.execute("""SELECT movieID FROM movie WHERE title=%s""", (m,))
        res = cursor.fetchone()    
        print(res)
        if res is None:
            print("movie with title '"+m+"' is not yet stored in db")
            cursor.execute("""INSERT INTO movie (title) VALUES (%s)""", (m,)) 
            db.commit() 
            last_movie_row_id = cursor.lastrowid
            cursor.execute("""INSERT INTO actor_movie (fk_actorID, fk_movieID) VALUES (%s, %s)""", (last_actor_row_id, last_movie_row_id)) 

        else:
            print("movie with title '"+m+"' is already in db with ID "+str(res))
            cursor.execute("""INSERT INTO actor_movie (fk_actorID, fk_movieID) VALUES (%s, %s)""", (last_actor_row_id, res[0]))

        print(res) 
        print(count)