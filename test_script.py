import os
import time
import csv
from functools import partial
from requests_html import HTMLSession
import pandas as pd
from multiprocessing import Pool
import copyrights_scraper

if __name__ == "__main__":
    session = HTMLSession()

    print(os.cpu_count())

    urls = []
    with open('youtube-charts-top-music-videos-mx-weekly-2022-07-07.csv', encoding='utf-8', mode='r') as f:
        csv_file = csv.reader(f)
        headers = next(csv_file)

        for row in csv_file:
            urls.append(row[-1])

    p = Pool(os.cpu_count())

    # Time Multiprocessing execution
    t1_parallel = time.perf_counter()
    results = p.map(partial(copyrights_scraper.get_license, session), urls)
    t_elapsed_parallel = time.perf_counter() - t1_parallel
    print(f'Parallel version time elapsed: {t_elapsed_parallel}')

    # Time Synchronous execution
    t1_serial = time.perf_counter()
    results_serial = [copyrights_scraper.get_license(url) for url in urls]
    t_elapsed_serial = time.perf_counter() - t1_serial
    print(f'Serial version time elapsed: {t_elapsed_serial}')

    for r in results:
        print(r)

    df = pd.DataFrame(results)
    df.to_csv('results.csv', index=False)