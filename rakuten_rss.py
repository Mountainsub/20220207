from lib.ddeclient import DDEClient
import pandas as pd
import numpy as np
import time 

 


def rss(item,k):
	


	dde_ware = []
	indexes = pd.read_csv("TOPIX_weight_jp.csv")
	
	#top1000 = pd.read_csv("topix1000.csv")
	jasd = pd.read_csv("japan-all-stock-data.csv")
	#.values.tolist() #文字列に変換 
	#indexes = np.array(indexes)
	indexes["コード"] = pd.to_numeric(indexes["コード"], errors='coerce')
	indexes_0 = indexes.dropna(subset=['コード'])

	indexes_code = indexes["コード"].astype(int)
	 # 2次元配列を1次元配列に変換

	#indexes["TOPIXに占める個別銘柄のウェイト"] = pd.to_numeric(indexes["TOPIXに占める個別銘柄のウェイト"], errors='coerce')
	#indexes_0 = indexes.dropna(subset=['TOPIXに占める個別銘柄のウェイト'])
	#indexes_weight = indexes["TOPIXに占める個別銘柄のウェイト"]
	

	for i,j in enumerate(indexes_code):
		indexes_code[i] = str(j) + ".T"
	indexes_code = np.array(indexes_code)
	indexes_code = indexes_code.flatten()
	ob= indexes["TOPIXに占める個別銘柄のウェイト"].copy()
	#indexes["TOPIXに占める個別銘柄のウェイト"] = indexes["TOPIXに占める個別銘柄のウェイト"].replace("%", "")
	

	for i,j in indexes.iterrows():
		#l = j.replace("%", "")
		indexes.at[i, "TOPIXに占める個別銘柄のウェイト"] = indexes.loc[i, "TOPIXに占める個別銘柄のウェイト"].replace("%", "")
		#if indexes.at[str(i), "TOPIXに占める個別銘柄のウェイト"] == j :
		#	print(i)		

	
	count = 0
	calc = 0
	issures = []
	for i,j in enumerate(indexes_code, start = k): #.Tは転置
		count += 1
		if i != 1461 and i != 420: # if i != 2166 and i != 1956 and i != 1461 and i != 420
			dde = DDEClient("rss", indexes_code[i])
			
			dde_ware.append(dde)
			#count += 1
			#print(str(i)+ dde.request("銘柄名称").decode("sjis"))
			calc += float(dde.request(item).decode("sjis")) * float(indexes["TOPIXに占める個別銘柄のウェイト"][i] )* 0.01
			#print(calc)
			del dde
			if count >= 126:
				#print("break")
				break
			if k == 2142 and count == 40:
				break 

			#issures.append(np.array(jasd["発行済株式数"][i]))
			#res += float(dde.request(item).decode("sjis")) * float(np.array(jasd["発行済株式数"][i])) #strip()		
		else:
			continue
			

	pocket = [calc, dde_ware, indexes["TOPIXに占める個別銘柄のウェイト"]]
	
	
	return pocket
	"""
	# ウエイトをかける
	
	while True:
		calc = 0
		
		#print(ddetopix_core30.request(item))
		t1 = time.time()
		for i, j in enumerate(dde_ware):
			calc += float(j.request(item).decode("sjis")) * indexes_weight[i] * 0.01
		t2 = time.time()
		print("所要時間:"+ str(t2 -t1) ,   calc )
	dde_ware = []
	
	return 0
	
	#　データのpreprocessing
	"""


def rss2(item,k, dde_ware, weights):
	calc = 0
	count = 0
	for i,j in enumerate(dde_ware): #.Tは転置
		if i != 2166 and i != 1956 and  i != 420:
			dde = dde_ware[i]
			#print(str(i)+ dde.request("銘柄名称").decode("sjis"))
			calc += float(dde.request(item).decode("sjis")) * float(weights[i] )* 0.01
			del dde
			count += 1
			if count >= 125:
				break
			#issures.append(np.array(jasd["発行済株式数"][i]))
			#res += float(dde.request(item).decode("sjis")) * float(np.array(jasd["発行済株式数"][i])) #strip()		
		else:
			continue
	return calc


def rss_dict(code, *args):
	"""
	楽天RSSから辞書形式で情報を取り出す(複数の詳細情報問い合わせ可）
	Parameters
	----------
	code : str
	args : *str
	Returns
	-------
	dict
	Examples
	----------
	>>>rss_dict('9502.T', '始値','銘柄名称','現在値')
	{'始値': '1739.50', '現在値': '1661.50', '銘柄名称': '中部電力'}
	"""

	dde = DDEClient("rss", str(code))

	
	values ={}
	element = []

	res = {}
	try:
		for item in args:
			res[item] = dde.request(item).decode('sjis').strip()
	except:
		print('fail: code@', code)
		res = {}
	finally:
		dde.__del__()
	return res

def fetch_open(code):
	""" 始値を返す（SQ計算用に関数切り出し,入力int）
	Parameters
	----------
	code : int
	Examples
	---------
	>>> fetch_open(9551)
	50050
	"""

	return float(rss(str(code) + '.T', '始値'))
