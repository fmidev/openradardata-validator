
## RODEO

The [RODEO project](https://rodeo-project.eu/) develops a user interface and Application Programming Interfaces (API) for accessing meteorological datasets declared as High Value Datasets (HVD) by the EU Implementing Regulation (EU) 2023/138 under the EU Open Data Directive (EU) 2019/1024. The project also fosters the engagement between data providers and data users for enhancing the understanding of technical solutions being available for sharing and accessing the HVD datasets.
This project provides a sustainable and standardized system for sharing real-time surface weather observations in line with the HVD regulation and WMO WIS 2.0 strategy. The real-time surface weather observations are made available through open web services, so that they can be accessed by anyone.

# Open Radar Data (ORD)

The weather radar data is also considered as HVDs, and therefore, one of the goals of RODEO is to supply near real-time weather radar observations. The radar data will be published on both a message queue using [MQTT](https://mqtt.org/) and [EDR](https://ogcapi.ogc.org/edr/) compliant APIs. Metadata will also be made available through [OGC Records](https://ogcapi.ogc.org/records/) APIs. The system architecture is portable, scalable and modular for taking into account possible future extensions to existing networks and datasets.

## Published datasets in ORD
There are three types of data available via ORD. 
1. European single-site radar data are available through the EUMETNET OPERA programme, both as a 24-hour rolling cache and as an extensive archive. The data are provided in BUFR format for older datasets and in ODIM HDF5 format for more recent ones.
2. European composite products — including maximum reflectivity factor, instantaneous rain rate, and 1-hour rainfall accumulation — are available both as a 24-hour rolling cache and as a long-term archive dating back to 2012. These products are provided by the EUMETNET OPERA programme in ODIM HDF5 and cloud-optimized GeoTIFF formats.
3. National radar product, e.g. national radar composites, rain rate composites, accumulation products, and echo tops. These are provided as aöonk to be downloaded from the national interfaces, and typically in ODIM HDF5 or cloud-optimized GeoTiffs.

### Requirements for sharing national products via ORD API:
•	National radar volume data is not shared via ORD API, only products. The data sharing is happening via OPERA to ORD API (if data sharing is authorized) 
•	Product format needs to be either ODIM H5 or cloud optimized GeoTiff 
•	Products are locally hosted in a national data store
•	Products can be accessible via an api or a public data store
•	Your interface to ORD is posting a json structured file with the required metadata and a link to the file for each ready product


## OpenRadarData Validator

The ORD system includes three endpoints for ingesting and sharing data:

### 1. BUFR Endpoint
- Used for uploading and sharing **BUFR files**.
- For **OPERA to ingest the European single site data** to European Weather Cloud S3 storage
- The ingester module:
  - Extracts metadata from BUFR files and stores it in the database.
  - Uploads the original (or renamed) BUFR file to the ORD S3 bucket.

### 2. ODIM Endpoint
- Processes **ODIM files**.
- For **OPERA to ingest the European single site data and OPERA composites** to European Weather Cloud S3 storage
- The ingester module:
  - Extracts metadata from ODIM files and stores it in the database.
  - Uploads the original (or renamed) ODIM file to the ORD S3 bucket.

### 3. JSON Endpoint
- Enables sharing **locally stored radar data**.
- For **National Meteorological Services (NMSs) to provide national products** via ORD
- Users provide radar metadata through the JSON endpoint.


**This tool** includes a JSON message generator for creating custom `json_upload_schema` files and a validator script to verify the schema. The message generator creates distinct JSON schemas for each quantity at each level.

### The 'level' attribute
Product | level value | notes
--- | --- | ---
SCAN | int(elangle *100) | Elevation of the SCAN
PVOL | int(elangle *100) | Elevation of the current dataset
PPI | Product parameter | Elevation angle used
CAPPI | Product parameter | Layer height above the radar
PCAPPI | Product parameter | Layer height above the radar
ETOP | Product parameter | Reflectivity level threshold
EBASE | Product parameter | Reflectivity level threshold
RHI | int(Product parameter *100) | Azimuth angle
VIL | Product parameter |  Top heights of the integration layer
PVOL | int(elangle *100) | Elevation of the current dataset
COMP | 0 | Other composites: CMAX, HMAX, etc...


## Installation
### Clone the repo
```shell
git clone https://github.com/EUMETNET/openradardata-validator.git
```
### Set python virtual envinronment
Create new python3 envinronment
```shell
cd openradardata-validator
python3 -m venv .venv
source .venv/bin/activate
```
Install requirements
```shell
pip install --upgrade pip
pip install -r ./requirements-odim.txt
pip install -r ./requirements-validator.txt
```
Set env value
```shell
export ORD_VALIDATOR_DIR=/path_to_validator_dir/
```
## Create shema
```shell
python3 ./odim2ordmsg.py /path_to_ODIM_file/ODIM_file.h5
```
## Schema validator

The upload_schema validation has two parts:
- **Entire schema validation**: validates the whole schema.
- **Radar metadata validation**: validates the `radar_meta` attribute, which contains radar product parameters such as elevation, PRF, vawelenght, etc. The `radar_meta` string value is a JSON object for flexibility, following the ODIM standard. It must include the mandatory format key which represents the file format.

Run the schema validator
```shell
python3 ./ord_validator.py ./examples/json/example_2_pttrc_SCAN_elevation_0.0_quantities_DBZH_TH_VRADH.json
```
The output:
```shell
Schemas: ['./schemas/openradardata-spec.json', './schemas/openradardata-radar_meta-spec.json', './examples/json/example_2_pttrc_SCAN_elevation_0.0_quantities_DBZH_TH_VRADH.json']
Read Openradar schema: ./schemas/openradardata-spec.json
Read Meta schema: ./schemas/openradardata-radar_meta-spec.json
Read msg: ./examples/json/example_2_pttrc_SCAN_elevation_0.0_quantities_DBZH_TH_VRADH.json
Validation OK: 2024-10-08T05:30:05Z 0-20000-0-08516     0       TH
Meta Validation OK
Validation OK: 2024-10-08T05:30:05Z 0-20000-0-08516     0       DBZH
Meta Validation OK
Validation OK: 2024-10-08T05:30:05Z 0-20000-0-08516     0       VRADH
Meta Validation OK
```
Where:
- ```Schemas```: list of shemas, first two are the validator schemas
- ```Read...```: reads the validator schemas
- ```Validation OK```: validates the each measure in the `json_upload_schema` and prints the `date` `level` and `quantity`
- ```Meta Validation OK```: validates the `radar_meta` attribute
