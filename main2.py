import subprocess
import asyncio
import multiprocessing
import threading
import pandas as pd
import numpy as np
import time 
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from request5.rakuten_rss import rss , rss2 
from lib.ddeclient import DDEClient
#並列処理






#dde_ware = []

def Main(k):
        
    calc = 0
        #dde_ware, calc = rss("現在値",k)[0], rss("現在値",k)[1]
    # print(k)
    return rss("現在値",k)    
        
    



def calculation(dde_ware, indexes_weight, num):
    t1 = time.time()
    calc = rss2("現在値",0, dde_ware, indexes_weight)
    #except Exception:
        #raise Exception     
    t2 = time.time()
     #print(calc)
    #print("\n"+ str(t2-t1))
    #print(t2)
    return calc 

if __name__ == '__main__':
    args = sys.argv # コマンドライン引数として開始地点のインデックスを数字で入力する
    #print(int(args[1]))
    count = 0
    firstLoop = args[2]
    if firstLoop=="T":
        array =  rss("現在値",int(args[1]))
        dde_ware, weight = array[1], array[2]
            # ware.append(dde_ware)
        firstLoop = "F" 
            # args[2]
        array = []
        t1 = time.time()
        while count == 0:
            count += 1
            array.append(t1)
            try:
                ex = calculation(dde_ware, weight, round(int(args[1]) / 126))
            except Exception:
                #time.sleep(0.2)
                ex = Exception
                while ex == Exception:
                    ex = calculation(dde_ware, weight, round(int(args[1]) / 126))
            else:
                if len(array) == 1:
                    print(ex)#,t2 - t1)
                else:
                    print(ex)#,array[-1] -array[-2])
        #while True:
            #calculation(dde_ware, weight, round(int(args[1]) / 126))
    else:
        array =  rss("現在値",int(args[1]))
        dde_ware, weight = array[1], array[2]
            # ware.append(dde_ware)
        firstLoop = "F" 
            # args[2]
        array = []
        t1 = time.time()
        while count == 0:
            count += 1
            try:
                ex = calculation(dde_ware, weight, round(int(args[1]) / 126))
            except Exception:
                #time.sleep(0.2)
                ex = Exception
                while ex == Exception:
                    ex = calculation(dde_ware, weight, round(int(args[1]) / 126))
            else:
                if len(array) == 1:
                    print(ex)#,t2 - t1)
                else:
                    print(ex)