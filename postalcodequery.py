import platform, requests, sqlite3, sys, time, timeit
from multiprocessing import Pool
import multiprocessing as mp
from matplotlib import pyplot as plt
import numpy as np

# Sector code map.
# Sector code map
def gen_legal_postal_codes():
    ref_sector_code = [
        1, 2, 3, 4, 5, 6      # D01
        7, 8,                 # D02
        14, 15, 16,           # D03
        9, 10,                # D04
        11, 12, 13,           # D05
        17,                   # D06
        18, 19,               # D07
        20, 21,               # D08
        22, 23,               # D09
        24, 25, 26, 27,       # D10
        28, 29, 30,           # D11
        31, 32, 33,           # D12
        34, 35, 36, 37,       # D13
        38, 39, 40, 41,       # D14
        42, 43, 44, 45,       # D15
        46, 47, 48,           # D16
        49, 50, 81,           # D17
        51, 52,               # D18
        53, 54, 55, 82,       # D19
        56, 57,               # D20
        58, 59,               # D21
        60, 61, 62, 63, 64,   # D22
        65, 66, 67, 68,       # D23
        69, 70, 71,           # D24
        72, 73,               # D25
        77, 78,               # D26
        75, 76,               # D27
        79, 80                # D28
        ]

    postal_codes = []
    for each in ref_sector_code:
        ### For Testing only, change the range to 10,000 for actual operation
        each_sector = range(0,10)
        #each_sector = range(0,10000)
        #each_sector = ['{0:06d}'.format(p+(each*10000)) for p in each_sector]
        each_sector = ['{0:06}'.format(p+(each*10000)) for p in each_sector]
        postal_codes = postal_codes + each_sector
    
    return postal_codes

def query_postal_code(postal_code):
    while True:
        try:
            resp = requests.get('https://developers.onemap.sg/commonapi/search?searchVal={0}&returnGeom=Y&getAddrDetails=Y&pageNum=1'
                            .format(postal_code)).json()
        except requests.exceptions.ConnectionError as e:
            print('Fetching {} failed. Retrying in 2 sec'.format(postal_code))
            
            time.sleep(2)
            continue
    
        if (resp['found'] == 1):
            conn = sqlite3.connect("postalcode.db")
            c = conn.cursor()
    
            c.execute("""INSERT INTO sgpostalcode(POSTAL, BLK_NO, ROAD_NAME, BUILDING, ADDRESS, X, Y, LATITUDE, LONGITUDE, LONGTITUDE)
                 VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")""" 
                 % (postal_code,
                    resp['results'][0]['BLK_NO'],
                    resp['results'][0]['ROAD_NAME'],
                    resp['results'][0]['BUILDING'],
                    resp['results'][0]['ADDRESS'],
                    resp['results'][0]['X'],
                    resp['results'][0]['Y'],
                    resp['results'][0]['LATITUDE'],
                    resp['results'][0]['LONGITUDE'],
                    resp['results'][0]['LONGTITUDE']))
            conn.commit()
            conn.close()
        
        break

def print_sysinfo():
    print('\nPython version  :', platform.python_version())
    print('compiler        :', platform.python_compiler())

    print('\nsystem     :', platform.system())
    print('release    :', platform.release())
    print('machine    :', platform.machine())
    print('processor  :', platform.processor())
    print('CPU count  :', mp.cpu_count())
    print('interpreter:', platform.architecture()[0])
    print('\n\n')

def serial(postal_codes):
    return [query_postal_code(postal) for postal in postal_codes]

def multiprocess(processes, postal_codes):
    pool = Pool(processes=processes)
    pool.map(query_postal_code, all_postal_codes)

def plot_results(benchmarks):
    bar_labels = ['serial', '2', '4', '8', '16', '32']

    fig = plt.figure(figsize=(10,8))

    # plot bars
    y_pos = np.arange(len(benchmarks))
    plt.yticks(y_pos, bar_labels, fontsize=16)
    bars = plt.barh(y_pos, benchmarks,
             align='center', alpha=0.4, color='g')

    # annotation and labels

    for ba,be in zip(bars, benchmarks):
        plt.text(ba.get_width() + 2, ba.get_y() + ba.get_height()/2,
                '{0:.2%}'.format(benchmarks[0]/be),
                ha='center', va='bottom', fontsize=12)

    plt.xlabel('time in seconds', fontsize=14)
    plt.ylabel('number of processes', fontsize=14)
    t = plt.title('Serial vs. Multiprocessing', fontsize=18)
    plt.ylim([-1,len(benchmarks)+0.5])
    plt.xlim([0,max(benchmarks)*1.1])
    plt.vlines(benchmarks[0], -1, len(benchmarks)+0.5, linestyles='dashed')
    plt.grid()

    plt.show()  
    
def delete_all_data():
    conn = sqlite3.connect("postalcode.db")
    c = conn.cursor()
    c.execute("DELETE from sgpostalcode")
    conn.commit()
    conn.close()

def multiprocessing(all_postal_codes):
    benchmarks = []
    
    delete_all_data()
    benchmarks.append(timeit.Timer('serial(all_postal_codes)',
                'from __main__ import serial, all_postal_codes').timeit(number=1))
    
    delete_all_data()
    benchmarks.append(timeit.Timer('multiprocess(2, all_postal_codes)',
                'from __main__ import multiprocess, all_postal_codes').timeit(number=1))
    
    delete_all_data()
    benchmarks.append(timeit.Timer('multiprocess(4, all_postal_codes)',
                'from __main__ import multiprocess, all_postal_codes').timeit(number=1))

    delete_all_data()
    benchmarks.append(timeit.Timer('multiprocess(8, all_postal_codes)',
                'from __main__ import multiprocess, all_postal_codes').timeit(number=1))

    delete_all_data()
    benchmarks.append(timeit.Timer('multiprocess(16, all_postal_codes)',
                'from __main__ import multiprocess, all_postal_codes').timeit(number=1))
    
    delete_all_data()
    benchmarks.append(timeit.Timer('multiprocess(32, all_postal_codes)',
                'from __main__ import multiprocess, all_postal_codes').timeit(number=1))
            
    plot_results(benchmarks)

if __name__ == '__main__':
    all_postal_codes = gen_legal_postal_codes()
    print(all_postal_codes)
    print_sysinfo() 
    
    # multiprocessing(all_postal_codes)
    delete_all_data()
    benchmarks.append(timeit.Timer('multiprocess(16, all_postal_codes)',
                'from __main__ import multiprocess, all_postal_codes').timeit(number=1))
  
    print_sysinfo()    
    
