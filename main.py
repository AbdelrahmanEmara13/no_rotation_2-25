# [START storage_download_file]
from google.cloud import storage
import pandas as pd
import datetime as dt
import asyncio 
from aiohttp_ip_rotator import RotatingClientSession


def download_blob(site):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()

    bucket = storage_client.bucket('holland_bkp')

    blob = bucket.blob('{}.txt'.format(site))
    blob.download_to_filename(site)


def to_date_obj(timestamp):
    return dt.datetime.strptime(timestamp[:14], '%Y%m%d%H%M%S')


non_html = ('.aac', '.abw', '.arc', '.avif', '.avi',
            '.azw', '.bin', '.bmp', '.bz', '.bz2', '.cda',
            '.csh', '.css', '.csv', '.doc', '.docx', '.eot',
            '.epub', '.gz', '.gif', '.ico', '.ics', '.jar',
            '.jpeg', '.jpg', '.js', '.jsonld', '.mid', '.midi',
            '.mjs', '.mp3', '.mp4', '.mpeg', '.mpkg', '.odp', '.ods',
            '.odt', '.oga', '.ogv', '.ogx', '.opus', '.otf', '.png',
            '.tar', '.tif', '.tiff', '.ts', '.ttf', '.vsd', '.wav',
            '.weba', '.webm', '.webp', '.woff', '.woff2', '.xls',
            '.xlsx', '.xul', '.zip', '.3gp', '.3g2', '.7z')

def pop_from_sites(site_name, sites_file):
    try:
            
        with open(sites_file, "r") as f:
            lines = f.readlines()
        with open(sites_file, "w") as f:
            for line in lines:
                if line.strip("\n") != site_name:
                    f.write(line)
    except Exception as e: print(e)
        
def tranform(file):

    try:

        df = pd.read_csv(file, header=None, delim_whitespace=True)
        df.columns = ["urlkey", "timestamp", "original",
                      "mimetype", "statuscode", "digest", "length"]
        df = df.astype({'timestamp': 'string'})

        df['date'] = df['timestamp'].apply(lambda x: to_date_obj(x))
        df['quarter'] = df['date'].dt.to_period('Q')
        df = df.loc[df.groupby(['quarter', 'urlkey'])['date'].idxmax()]
        df = df[~df.original.str.endswith(non_html)]
        df.drop(columns=['mimetype', 'statuscode', 'length'], inplace=True)

        df['raw_url'] = "https://web.archive.org/web/" + \
            df['timestamp']+"id_/"+df['original']

        return df

    except Exception as e:
        print(e)


def populate(digest):
    try:
            
        f=open(digest,'r', encoding="utf-8")
        return f.read()
    except:
        pass

async def fetch(df):
    async with RotatingClientSession(
        "http://web.archive.org",
        "AKIAZ77KOHU3FBNF2XT7",
        "aG7HwcOTdKV3mOAPC6oTMUvntBEgPCgntMC4SRpc"
    ) as session:
      
        for ind in df.index:
            try:
                file_name=df['digest'][ind]
                raw_url=df['raw_url'][ind]
                response = await session.get(raw_url)
                if response.status== 200:
                    with open(file_name, "w", encoding="utf-8") as f:
                        f.write(await response.text())
                    # f = open(f"{file_name}", "a")
                    # f.write(await response.text())
                    # f.close()
            except Exception as e: print(e)


if __name__ == "__main__":
    sites_file = open('sites.txt', 'r')
    files = sites_file.read().split()
    files_done=1
    for file in files:
            try:

                # file='101-commerce.com'
                download_blob(file)  # saves file from bucket
                print('{} downloaded...'.format(file))
                df = tranform(file)  # converts file to df

                # df.to_csv('trans_df.csv')
                print('{} transformed to df'.format(file))
                
                unique_digest_df = df.drop_duplicates(subset='digest')

                # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
                asyncio.run(fetch(unique_digest_df))
                print('{} populated...'.format(file))
                df['content']= df['digest'].apply(populate)
                df.to_csv('{}.csv'.format(file))
                pop_from_sites(file, 'sites.txt')
                print(f'files done: {files_done}')
                files_done+=1

            except Exception as e: print(e)

            
            
