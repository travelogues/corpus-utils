""" 
This script is used for downloading large numbers of books from the SACHA interface. 
It makes use of the multiprocessing library to speed up the process. All cores but 
one are used to download in parallel.
"""

import csv
import json
import logging
import requests
import time
from multiprocessing import Pool, cpu_count

logging.basicConfig(handlers=[logging.FileHandler('sacha_multi.log'),
                              logging.StreamHandler()],
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p')

def download(barcode):
    start_time = time.time()
    logging.info('%s - now requesting.' % barcode)
    r = requests.get('https://iiif.onb.ac.at/presentation/ABO/%s/manifest/' % barcode)

    if r.status_code != 200:
        logging.critical('%s - requesting produced a HTTP %s' % (barcode, r.status_code))
    elif r.status_code == 200:
        manifest = json.loads(r.text)

        # store the meta data
        # with open('%s/metadata/%s.meta' % (target_dir, barcode[1:]), 'w', encoding='utf-8') as metadata:
        #    json.dump(manifest['metadata'], fp=metadata, sort_keys=True, indent=4)

        # store content
        with open('%s/books/Z%s.txt' % (target_dir, barcode[1:]), 'w', encoding='utf-8') as fulltext:
            # iterate the manifest
            pages = len(manifest['sequences'][0]['canvases'])
            # for canvas in tqdm(manifest['sequences'][0]['canvases']):
            for canvas in manifest['sequences'][0]['canvases']:
                content = canvas['otherContent'][0]['resources'][0]['resource']['@id']
                print(content)
                if content[-3:] == 'txt':
                    canvas_text = requests.get(content)
                    # quick fix for ONB switch to https, as this is not changed in the manifests yet
                    # canvas_text = requests.get('https' + content[4:])
                    status = canvas_text.status_code
                    if status == 200:
                        pass
                    else:
                        logging.critical('%s - requesting produced a HTTP %s' % (content, status))
                    fulltext.write(canvas_text.text)
                    fulltext.write('\n')
                # TODO: change according to the current system!
                # Aim for a maximum of 400 requests per second, depending on the number of cores available.
                time.sleep(0.01)

            duration = time.time() - start_time
            average = duration / pages
            logging.info('%s - successfully stored to disk. Time needed: %.2fs for %d pages (averaging %.2f seconds per page).'
                         % (barcode, duration, pages, average))
    time.sleep(0.01)


if __name__ == '__main__':
    target_dir = '.'
    pool = Pool(processes=cpu_count()-1)  # uses all available cores minus 1 (to prevent locking the system)

    for barcode in ['Z202217401', 'Z255404601', 'Z179802809', 'Z162238509', 'Z172664006', 'Z7548400', 'Z17598520X', 'Z162727909', 'Z158154002', 'Z7521704', 'Z160975100', 'Z124194406', 'Z258872504', 'Z148215301', 'Z167593002', 'Z98083708', 'Z183953309', 'Z185660103', 'Z151691701', 'Z258328300', 'Z124194303', 'Z116250504', 'Z206550509', 'Z184034208', 'Z174670204', 'Z178264701', 'Z169598106', 'Z7548606', 'Z2589170X', 'Z185658509', 'Z7549003', 'Z257366408', 'Z180968703', 'Z136909009', 'Z222154609', 'Z35027809', 'Z204851208', 'Z179802500', 'Z260093605', 'Z174866901', 'Z186337804', 'Z25846201', 'Z255402409', 'Z166753708', 'Z177063608', 'Z184069909', 'Z183441505', 'Z158093207', 'Z20250900X', 'Z164893400', 'Z169598209', 'Z158092902', 'Z7548801', 'Z162727806', 'Z157798104', 'Z17935770X', 'Z158093001', 'Z179803000', 'Z258327502', 'Z185658601', 'Z25484910X', 'Z160634503', 'Z176181002', 'Z155616709', 'Z223598805', 'Z27285000', 'Z177818309', 'Z16672230X', 'Z98081402', 'Z17474490X', 'Z174007102', 'Z170542803', 'Z155531200', 'Z116298604', 'Z179585409', 'Z152203507', 'Z176601702', 'Z184408406', 'Z260093502', 'Z173359400', 'Z178041608', 'Z166722104', 'Z2586270X', 'Z177021201', 'Z22143670X', 'Z180282507', 'Z207699008', 'Z184034105', 'Z158093104', 'Z255404704', 'Z174993108', 'Z17467040X', 'Z163695102', 'Z35073807', 'Z179802603', 'Z174745307', 'Z174745204', 'Z177006005', 'Z175985302', 'Z15871150X', 'Z158711407', 'Z157798001', 'Z151691804', 'Z173150705', 'Z255428101', 'Z162202801', 'Z116298501', 'Z166767100', 'Z138830305', 'Z177767703', 'Z160975008', 'Z166258709', 'Z223449009', 'Z119559800', 'Z42126205', 'Z15809330X', 'Z18621150X', 'Z167219001', 'Z96106900', 'Z22359860X', 'Z174799705', 'Z166258606', 'Z205462704', 'Z56325300', 'Z179802706', 'Z15261107', 'Z156569703', 'Z183502105', 'Z203608902', 'Z220809805', 'Z179802901', 'Z26009340X', 'Z25828107', 'Z176392001', 'Z157659700', 'Z184084601', 'Z258340208', 'Z151647700', 'Z98081700', 'Z173456909', 'Z186338006', 'Z35027901', 'Z163129601', 'Z2460670X', 'Z15265204', 'Z164893308', 'Z256457406', 'Z256457303', 'Z43138203', 'Z206925803', 'Z151691609', 'Z224525901', 'Z168938207', 'Z206853506', 'Z174223005', 'Z116299001', 'Z164891907', 'Z202322208', 'Z137242205', 'Z179357802', 'Z158154208', 'Z165954408', 'Z16744690X', 'Z255428204', 'Z7548503', 'Z137153208', 'Z224686301', 'Z207698909', 'Z136422406', 'Z260071208', 'Z165953209', 'Z161848602', 'Z124902206', 'Z98081803', 'Z184071801', 'Z255404509', 'Z18407730X']:
        pool.apply_async(download, args=(barcode,))

    pool.close()
    pool.join()
