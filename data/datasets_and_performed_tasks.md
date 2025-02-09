# Datasets with Tasks
***

### Car Accidents

| Location                                          |  Size  | Description                                                                                                          | Source                                                                                                             | 
|---------------------------------------------------|:------:|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| ```/user/s2739046/MBD/ongevallen.csv```           | 357 MB | All car accidents in the Netherlands, from 2014 up until 2023, 1193484 entries                                       | https://maps.rijkswaterstaat.nl/dataregister/srv/eng/catalog.search#/metadata/4gqrs90k-vobr-5t59-x726-4x2unrs1vawz |
| ```/user/s2692759/MBD/ongevallen_gefilterd.csv``` | 108 MB | All car accidents in the Netherlands, from 2014 up until 2023, filtered out null columns, 1193484 entries            |                                                                                                                    |
| ```/user/s2739046/MBD/partijen.csv```             | 213 MB | All parties in car accidents in the Netherlands, from 2014 up until 2023, 1801034 entries                            | https://maps.rijkswaterstaat.nl/dataregister/srv/eng/catalog.search#/metadata/4gqrs90k-vobr-5t59-x726-4x2unrs1vawz |
| ```/user/s2692759/MBD/partijen_gefilterd.csv```   | 109 MB | All parties in car accidents in the Netherlands, from 2014 up until 2023, filtered out null columns, 1801034 entries |                                                                                                                    |
| ```/user/s2739046/MBD/puntlocaties.txt```         | 109 MB | All location coordinates of road sections from previous tables, from 2014 up until 2023                              |                                                                                                                    |

**Tasks performed:**
- Top 20 accidents per local authority
- Accidents per year
- Accidents per year with outcome
- Accidents per local authority map
- Accidents with nature of accident
- Accidents with amount of parties

***

### Road Section Information

| Location                                          |  Size  | Description                                                                            | Source                                                                                | 
|---------------------------------------------------|:------:|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| ```/user/s2692759/MBD/wegvakken.csv```            | 964 MB | Road section information per ID, which are also in car accident table, 1573304 entries | https://data.overheid.nl/en/dataset/47092-nwb-wegen---wegvakken--rws-#panel-resources |
| ```/user/s2692759/MBD/ongevallen_wegvakken.csv``` | 881 MB | Road section information combined with accidents, 572781 entries                       |                                                                                       |

**Tasks performed:**
- Top 10 BST (types of roads) bar chart
- Type of roads (A, N, etc.) frequencies bar chart

***

### Speed Limits

| Location                                            |  Size  | Description                     | Source                                              |
|-----------------------------------------------------|:------:|---------------------------------|-----------------------------------------------------|
| ```/user/s2563363/FilesProject/Snelheden.parquet``` | 278 MB | Speed Limits in the Netherlands | https://www.nationaalwegenbestand.nl/nwb-downloaden |

***

### Trees Next To Roads

| Location                                               | Size  | Description                                  | Source                                              | 
|--------------------------------------------------------|:-----:|----------------------------------------------|-----------------------------------------------------|
| ```/user/s2563363/FilesProject/wkd_012-BMN_AANT.csv``` | 72 MB | Trees next to roads with distances from road | https://www.nationaalwegenbestand.nl/nwb-downloaden |

**Tasks performed:**
- Amount of accidents correlated with amount of trees

***

### Population Density

| Location                                         |   Size   | Description                                            | Source                                                                        |
|--------------------------------------------------|:--------:|--------------------------------------------------------|-------------------------------------------------------------------------------|
| ```/user/s2692759/MBD/bevolkingsdichtheid.csv``` |  5.2 KB  | Population density per municipality in the Netherlands | https://www.cbs.nl/nl-nl/visualisaties/dashboard-bevolking/regionaal/inwoners |
