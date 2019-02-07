import MySQLdb
from MySQLdb import connect
import requests
import re
import string
import sys, traceback


def initDBConnection(host, user, pw, dbname):
    return connect(host, user, pw, dbname)

def getMovies(db_cursor, actor_id):
    actor_movies_list = []    
    db_cursor.execute("""SELECT fk_movieID FROM actor_movie WHERE fk_actorID=%s""", (str(actor_id), ))
    actor_movies = db_cursor.fetchall()

    for i in range(len(actor_movies)):
        actor_movies_list.append(actor_movies[i][0])

    return actor_movies_list


def compareMovies(db, db_cursor, actor1_id, actor2_id):
    actor1_movies_list = []
    actor1_movies_list = getMovies(db_cursor, actor1_id)

    actor2_movies_list = []
    actor2_movies_list = getMovies(db_cursor, actor2_id) 


    result_list = []
    result_list = [k for k in actor1_movies_list if k in actor2_movies_list]
    print(result_list)
    #if i is 100:
    #    break
    if result_list:
        print("found corresponding movies: "+str(result_list)+" for actor1 ID "+str(actor1_id)+" and actor2 ID "+str(actor2_id))
        #cursor.execute("""UPDATE actor SET bacon_number = 1 WHERE actorID = %s AND NOT actorID = 97""", (str(actor1_id),))
        #db.commit()
        return True       
    else:
        print("no movies corresponding: "+str(result_list)+" for actorID "+str(actor1_id)+" and actor2 ID "+str(actor2_id))
        return False

def setBaconNum(db, db_cursor, actor1_id, actor2_id):
    
    if compareMovies(db, db_cursor, actor1_id, actor2_id):
        print("determine Bacon index number for actors...")

        db_cursor.execute("""SELECT bacon_number FROM actor WHERE actorID = %s""", (str(actor1_id),))
        res1 = db_cursor.fetchone()

        db_cursor.execute("""SELECT bacon_number FROM actor WHERE actorID = %s""", (str(actor2_id),))
        res2 = db_cursor.fetchone()
        
        
        if res1[0] is None and res2[0] is None:
            print("actor1 and 2 bacon number are NULL. Nothing to do.")
            return
        elif res1[0] is not None and res2[0] is None:
            print("actor2 bacon number ("+str(res1[0])+") is NULL and actor1 bacon number is ("+str(res2[0])+". Setting bacon number for actor2 to actor1 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s""", (str(res1[0]+1), str(actor2_id)))
            db.commit()
            return
        elif res2[0] is not None and res1[0] is None:
            print("actor1 bacon number ("+str(res2[0])+") is NULL and actor2 bacon number is ("+str(res2[0])+". Setting bacon number for actor1 to actor2 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s""", (str(res2[0]+1), str(actor1_id)))
            db.commit()
            return
        elif res1[0] == res2[0]:
            print("actor1 and 2 bacon number are equal. Nothing to do.")
            return
        elif res1[0] > (res2[0]+1):
            print("actor1 bacon number ("+str(res1[0])+") is bigger by at least +2 than actor2 bacon number ("+str(res2[0])+". Setting bacon number for actor1 to actor2 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s""", (str(res2[0]+1), str(actor1_id)))
            db.commit()
            return
        elif res2[0] > (res1[0]+1):
            print("actor2 bacon number ("+str(res2[0])+") is bigger by at least +2 than actor1 bacon number ("+str(res2[0])+". Setting bacon number for actor2 to actor1 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s""", (str(res1[0]+1), str(actor2_id)))
            db.commit()
            return

db = initDBConnection("127.0.0.1", "root", "", "bacon")

cursor = db.cursor()

res = None
bacon = None

cursor.execute("""SELECT actorID FROM actor WHERE name=%s""", ('Kevin Bacon',))

bacon = cursor.fetchone()

print(bacon[0])

cursor.execute("""SELECT fk_movieID FROM actor_movie WHERE fk_actorID=%s""", (bacon,))

bacon_movies = cursor.fetchall()

print(bacon_movies)

bacon_movie_list = []

for i in range(len(bacon_movies)):
    bacon_movie_list.append(bacon_movies[i][0])

print(bacon_movie_list)


cursor.execute("""SELECT MAX(actorID) FROM actor""")

last_actor_id = cursor.fetchone()

print(last_actor_id[0])

cursor.execute("""SELECT bacon_number FROM actor WHERE actorID = 1""")

result = cursor.fetchone()
print(result)
print(result[0])

#traceback.print_exc(file=sys.stdout)
#sys.exit(0)

for i in range(int(last_actor_id[0])):
    for j in range(int(last_actor_id[0])):
        k = j+i+1
        if k > int(last_actor_id[0]):
            break
        if i == j:
            continue
        setBaconNum(db, cursor, i, k)



