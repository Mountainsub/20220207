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
def get_lines(cmd):
    '''
    :param cmd: str 実行するコマンド.
    :rtype: generator
    :return: 標準出力 (行毎).
    '''
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    #while True
    line = proc.stdout.readline()
    if line:
            yield line
    return





#dde_ware = []

def Main(k):
        
    calc = 0
        #dde_ware, calc = rss("現在値",k)[0], rss("現在値",k)[1]
    #print(k)
    return rss("現在値",k)    
        
    



def calculation(dde_ware, indexes_weight):
        #t1 = time.time()
    calc = rss2("現在値",0, dde_ware, indexes_weight)
            
        #t2 = time.time()
    return calc

if __name__ == '__main__':  
    count = 0
    firstLoop = True
    temp = 0
    t1 = time.time()
    for i in range(1):
        while count <= 2142:
            if firstLoop:
                first = "T"
                for line in get_lines(cmd='python main2.py ' + str(count)+ ' '+ str(first)): # ファイル読み込み　第一引数はスタートナンバー                
                    string = "file_"+ str(int(18 * 0 + round(count / 126))) + ".txt"
                    f = open(string, 'w')
                    f.write(line.decode('sjis')) 
                    #print(count)
                    #time.sleep(0.1)
                #firstLoop = False
                firststep = True
            else:
                for line in get_lines(cmd='python main2.py ' + str(count)+ ' '+ "F"): # ファイル読み込み　第一引数はスタートナンバー                
                    string = "file_"+ str(int(0 * temp + round(count / 126))) + ".txt"
                    f = open(string, 'w')
                    f.write(line.decode('sjis')) 
                    #print(count)
                    #time.sleep(0.1)
                firststep = True
            count += 126
            if count == 2142:
                """
                while firststep:
                    temp = 0
                    while temp <= 1000:
                        temp += 1
                    firststep = False
                    continue
                """
                proc = subprocess.Popen('python main3.py', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                #count = 0
                
            #break
        #while True
                result = proc.communicate()[0].decode('sjis')
                break
                #print(result)
                #temp += 1
        t2 = time.time()
        print("経過時間:"+ str(t2-t1),result)
        count = 0
        
    



