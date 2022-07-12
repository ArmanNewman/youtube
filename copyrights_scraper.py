import json
from typing import Dict
from requests_html import HTMLSession

def get_license(session, url: str) -> Dict:
    params = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5',
        'Content-Language': 'en-US'
    }
    r = session.get(url, params=params)
    # print(r.status_code)

    scripts = r.html.find('script')
    
    def check_if_license(txt: str) -> bool:
        return ('"simpleText":"LICENSES"') in txt or ('"simpleText":"LICENCIAS"' in txt)

    target_script = [script for script in scripts if check_if_license(script.text)]
    
    if not target_script:
        return {
            'vid_url': url,
            'metadata_title': 'Not available',
            'metadata_text': 'Not available'
        }

    txt = target_script[0].text.strip().split(' = ')[-1][:-1]
    data = json.loads(txt)
    for elem in data['engagementPanels']:
        try:
            a = elem['engagementPanelSectionListRenderer']['content']['structuredDescriptionContentRenderer']['items']
            for item in a:
                try:
                    b = item['videoDescriptionMusicSectionRenderer']
                    c = b['carouselLockups'][0]['carouselLockupRenderer']['infoRows'][-1]
                    info = c['infoRowRenderer']
                    info_title = info['title']['simpleText']
                    info_meta = info['expandedMetadata']['simpleText']
                    return {
                        'vid_url': url,
                        'metadata_title': info_title,
                        'metadata_text': info_meta
                    }
                except KeyError:
                    continue
        except KeyError:
            continue

if __name__ == '__main__':
    import os
    import time
    import csv
    import pandas as pd
    from multiprocessing import Pool

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
    results = p.map(get_license, urls)
    t_elapsed_parallel = time.perf_counter() - t1_parallel
    print(f'Parallel version time elapsed: {t_elapsed_parallel}')
    
    # Time Serialized execution
    t1_serial = time.perf_counter()
    results_serial = [get_license(url) for url in urls]
    t_elapsed_serial = time.perf_counter() - t1_serial
    print(f'Serial version time elapsed: {t_elapsed_serial}')

    for r in results:
        print(r)
    
    df = pd.DataFrame(results)
    df.to_csv('results.csv', index=False)