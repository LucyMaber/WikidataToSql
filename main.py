import signal
from qwikidata.entity import WikidataItem
from qwikidata.json_dump import WikidataJsonDump
from qwikidata.utils import dump_entities_to_json
from database import DatabaseHandler
from alive_progress import alive_bar
import atexit
import time
import random
sql_ = DatabaseHandler()

try:
    file = open("count.txt", "r")
    count = int(file.readline())
    file.close()
except:
    count = 0


def on_exit():
    sql_.conn.commit()
    sql_.conn.close()
    print("Database closed")
    file = open("count.txt", "w")
    file.write(str(count))
    file.close()


def save_count():
    sql_.conn.commit()
    file = open("count.txt", "w")
    file.write(str(count))
    file.close()


atexit.register(on_exit)
import os
block_exit = False
waiting_exit = False


def handler(signum, frame):
    global waiting_exit
    global block_exit
    if block_exit:
        print("waiting for block to finish")
    else:
        save_count()
        exit()
    waiting_exit = True
    


signal.signal(signal.SIGINT, handler)
wjd_dump_path = "/home/william/Code/code_reddit/big_data/wikidata-20220103-all.json.gz"
lasy_time = 0
totle_time = 0
M_count = 0
def main():
    global block_exit
    global waiting_exit
    global wjd_dump_path
    global lasy_time
    global totle_time
    global M_count
    global count
    with alive_bar(107621242-count) as bar:
        wjd = WikidataJsonDump(wjd_dump_path)
        for ii, entity_dict in enumerate(wjd):
            if ii == 0 and count != 0:
                print("skipping to", count,"remaining", 107621242-count)
            if count <= ii:
                M_count = M_count + 1
                start_time_B = time.time()
                block_exit = True
                sql_.insert_entitys(entity_dict, location="wikidata")
                bar()
                count = count + 1
                block_exit = False
                if waiting_exit:
                    on_exit()
                    exit()
                if count % 500 == 0:
                    end_time_A = time.time()
                    time_taken = end_time_A - start_time_B
                    totle_time +=time_taken
                    print("done 500 count:", count,
                          "remaining:", 107621242-count, "lasy_time:", lasy_time, "time_taken:", time_taken)
                    save_count()
                    sql_.conn.commit()
                    if time_taken > 5:
                        lasy_time += random.random()
                    elif time_taken < 5:
                        lasy_time -= random.random()
                else:   
                    end_time_A = time.time()
                    time_taken = end_time_A - start_time_B
                    totle_time +=time_taken
                    if time_taken < 0.034:
                        lasy_time += random.random()
                    elif time_taken > 0.039:
                        lasy_time -= random.random()
                lasy_time = max(0, lasy_time)
                if lasy_time != 0:
                    print("sleeping for", lasy_time)
                    time.sleep(lasy_time)
                    # time.sleep(1-time_taken)
                # get size of data to be committed
            else:
                if count % 500 == 0:
                    print("starting at", count, "skipping to", count)
            
if __name__ == "__main__":
    main()
