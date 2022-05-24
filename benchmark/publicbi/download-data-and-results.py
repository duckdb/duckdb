import urllib.request
import os

datadir = 'data'
for subdir, dirs, files in os.walk(datadir):
    for file in files:
        if file.endswith('.table.sql'):
            if "tables" in subdir:
                subset = file.split(".table.sql")[0]
                # if subset == "Arade_1": - Uncomment and tab all lines below to download specific dataset
                print(f"Downloading {subset}")
                try:
                    urllib.request.urlretrieve(f"""https://zenodo.org/record/6277287/files/{subset}.csv.gz""", subset + ".csv.gz")
                except:
                    print(f"{subset} not found on Zenodo part 1")
                    try:
                        urllib.request.urlretrieve(f"""https://zenodo.org/record/6344717/files/{subset}.csv.gz""", subset + ".csv.gz")
                    except:
                        print(f"{subset} not found on Zenodo part 2")
