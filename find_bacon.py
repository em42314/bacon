import MySQLdb
from MySQLdb import connect
import requests
import re
import string
import sys, traceback
#from threading import Thread, Lock
import threading
import queue
import time


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
    #print(result_list)
    #if i is 100:
    #    break
    if result_list:
        #print("found corresponding movies: "+str(result_list)+" for actor1 ID "+str(actor1_id)+" and actor2 ID "+str(actor2_id))
        #cursor.execute("""UPDATE actor SET bacon_number = 1 WHERE actorID = %s AND NOT actorID = 97""", (str(actor1_id),))
        #db.commit()
        return True       
    else:
        #print("no movies corresponding: "+str(result_list)+" for actorID "+str(actor1_id)+" movieList "+str(actor1_movies_list)+" and actor2 ID "+str(actor2_id)+" movieList "+str(actor2_movies_list))
        return False

def setBaconNum(db, db_cursor, actor1_id, actor2_id):
    
    if compareMovies(db, db_cursor, actor1_id, actor2_id):
        #print("determine Bacon index number for actors...")
        #print("actorID 1 is "+str(actor1_id)+" and actorID 2 is "+str(actor2_id))
        db_cursor.execute("""SELECT bacon_number FROM actor WHERE actorID = %s""", (str(actor1_id),))
        res1 = db_cursor.fetchone()

        db_cursor.execute("""SELECT bacon_number FROM actor WHERE actorID = %s""", (str(actor2_id),))
        res2 = db_cursor.fetchone()
        
        
        if res1[0] is None and res2[0] is None:
            #print("actor1 and 2 bacon number are NULL. Nothing to do.")
            return
        elif res1[0] is not None and res2[0] is None:
            #print("actor2 bacon number ("+str(res1[0])+") is NULL and actor1 bacon number is ("+str(res2[0])+". Setting bacon number for actor2 to actor1 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s AND bacon_number IS NULL""", (str(res1[0]+1), str(actor2_id)))
            db.commit()
            return
        elif res2[0] is not None and res1[0] is None:
            #print("actor1 bacon number ("+str(res2[0])+") is NULL and actor2 bacon number is ("+str(res2[0])+". Setting bacon number for actor1 to actor2 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s AND bacon_number IS NULL""", (str(res2[0]+1), str(actor1_id)))
            db.commit()
            return
        elif res1[0] == res2[0]:
            #print("actor1 and 2 bacon number are equal. Nothing to do.")
            return
        elif res1[0] > (res2[0]+1):
            #print("actor1 bacon number ("+str(res1[0])+") is bigger by at least +2 than actor2 bacon number ("+str(res2[0])+". Setting bacon number for actor1 to actor2 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s AND bacon_number > %s""", (str(res2[0]+1), str(actor1_id), str(res2[0]+1)))
            db.commit()
            return
        elif res2[0] > (res1[0]+1):
            #print("actor2 bacon number ("+str(res2[0])+") is bigger by at least +2 than actor1 bacon number ("+str(res2[0])+". Setting bacon number for actor2 to actor1 bacon number +1.")
            db_cursor.execute("""UPDATE actor SET bacon_number = %s WHERE actorID = %s AND bacon_number > %s""", (str(res1[0]+1), str(actor2_id), str(res1[0]+1)))
            db.commit()
            return


dbc = initDBConnection("127.0.0.1", "root", "", "bacon")

dbc_cursor = dbc.cursor()

dbc_cursor.execute("""SELECT MAX(actorID) FROM actor""")

last_actor_id = dbc_cursor.fetchone()

print(last_actor_id[0])

#traceback.print_exc(file=sys.stdout)
#sys.exit(0)

range_max = int(last_actor_id[0])

print(range_max)
#global range_cur = 1

#time.sleep(5)
    
exitFlag = 0

class myThread (threading.Thread):
   def __init__(self, threadID, name, q, max_range, db_con):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.q = q
      self.max_range = max_range
      self.db_con = db_con
      self.db_cursor = self.db_con.cursor()      
   def run(self):
      print ("Starting " + self.name)
      process_data(self.name, self.q, self.max_range, self.db_con, self.db_cursor)
      print ("Exiting " + self.name)

def process_data(threadName, q, r, d, c):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            start_time = time.time()            
            data = q.get()
            queueLock.release()
            fileLock2.acquire()       
            f = open('C:/git/bacon/threads.log', 'a')
            f.write("thread "+str(threadName)+" starts processing work item "+str(data)+"\n")
            f.close()    
            fileLock2.release()
            print ("%s processing %s" % (threadName, data))        
            print("comparing actor with actorID "+str(data))            
            for m in range(r):
                if (m % 1000) == 0:
                    fileLock2.acquire()
                    f = open('C:/git/bacon/threads.log', 'a')
                    f.write("thread "+str(threadName)+" processed comparison actor1 "+str(data)+" with actor2 "+str(m)+"\n")
                    f.close
                    fileLock2.release()
                    #k = j+i+1
                    #if k > int(last_actor_id[0]):
                    #   break
                if data == m:
                    continue
                setBaconNum(d, c, data, m)
            stop_time = time.time()
            fileLock.acquire()
            f = open('C:/git/bacon/output.log', 'a')
            f.write("time for actorID "+str(data)+" is "+str(stop_time - start_time)+" processed by thread "+str(threadName)+"\n")  
            f.close()  
            fileLock.release()
            fileLock2.acquire()
            f = open('C:/git/bacon/threads.log', 'a')
            f.write("thread "+str(threadName)+" finished processing work item "+str(data)+"\n")
            f.close
            fileLock2.release()            
        else:
            queueLock.release()
            time.sleep(1)

threadList = []

#f = open('output.log', 'w',)

f = open('C:/git/bacon/output.log', 'w')
f.write('')
f.close()

f = open('C:/git/bacon/threads.log', 'w')
f.write('')
f.close()

for i in range(100):
    threadList.append('Thread-'+str(i))

queueLock = threading.Lock()
fileLock = threading.Lock()
fileLock2 = threading.Lock()
workQueue = queue.Queue(0)
threads = []
threadID = 1

#db_list = []

# Create new threads
for tName in threadList:    
    thread = myThread(threadID, tName, workQueue, range_max, initDBConnection("127.0.0.1", "root", "", "bacon"))
    thread.start()
    threads.append(thread)
    threadID += 1
#time.sleep(5)
# Fill the queue
queueLock.acquire()
for i in range(range_max):
    print("filling work queue with item: "+str(i))
    workQueue.put(i)
    #print(str(workQueue.get()))
queueLock.release()

# Wait for queue to empty
while not workQueue.empty():
   pass

# Notify threads it's time to exit
exitFlag = 1

# Wait for all threads to complete
for t in threads:
   t.join()
print ("Exiting Main Thread")




#for i in range(int(last_actor_id[0])):
#    for j in range(int(last_actor_id[0])):
#        k = j+i+1
#        if k > int(last_actor_id[0]):
#            break
#        if i == j:
#            continue
#        setBaconNum(db, cursor, i, k)



