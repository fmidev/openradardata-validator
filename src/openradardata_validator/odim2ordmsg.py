import copy
import io
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import h5py
import numpy
import pandas as pd

from openradardata_validator.radar_cf import radar_cf

current_filedir = Path(__file__).parent.resolve()

schema_dir = current_filedir / "schemas"
radardb_dir = current_filedir / "stations"

DEFAULT_WIGOS = "0-0-0-"
DEFAULT_WMO = "0-20000-0-"
test_schema_path = schema_dir / "odim_to_e_soh_message.json"
DEFAULT_URL = "https://My-default-URL"

S3_BUCKET_NAME = "My-S3-Bucket"
S3_ACCESS_KEY = ""
S3_SECRET_ACCESS_KEY = ""
S3_ENDPOINT_URL = "https://My-Data-Server.com"

# radars_format = {"WMO Code": "Int32" }
radars_format = {"WIGOS Station Identifier": "str"}

default_data_link = {
    "href": "",
    "length": 0,
    "rel": "items",
    "title": "Default link, to data.",
    "type": "application/x-odim",
}


def init_radars(fname: Path = radardb_dir / "OPERA_RADARS.csv") -> pd.DataFrame:
    # print("Init OPERA Radar database")
    ret = pd.read_csv(fname, header=0, keep_default_na=False, dtype=radars_format)
    return ret


def get_attr(field: h5py.Group, key: str) -> Any:
    ret = None
    if key in field.attrs:
        ret = field.attrs[key]
        # Array type workaround: return the first element if len == 1
        if isinstance(ret, numpy.ndarray):
            if len(ret) == 1:
                ret = ret[0]
    return ret


def get_attr_str(field: h5py.Group, key: str) -> str | None:
    ret = get_attr(field, key)
    if ret is None:
        return ret
    if isinstance(ret, str):
        return ret
    if isinstance(ret, numpy.number):
        return str(ret)
    return str(ret.decode("utf-8"))


def find_source_type(source: str, sid: str) -> str:
    ret = ""
    source_list = source.split(",")
    for source_element in source_list:
        s = source_element.split(":")
        if s[0] == sid:
            ret = s[1]
            break
    return ret


def odim_datetime(odate: bytes, otime: bytes) -> datetime:
    dstr = (odate + otime).decode("utf-8")
    dt = datetime.strptime(dstr, "%Y%m%d%H%M%S")
    return dt


def set_meta(
    m_dest: dict[str, str | int | float],
    m_src: str,
    m_attrs: list[str],
    fmt: str = "str",
) -> None:
    for meta in m_attrs:
        meta_val = get_attr_str(m_src, meta)
        if meta_val is not None and len(meta_val):
            match fmt:
                case "str":
                    m_dest[meta] = str(meta_val)
                case "int":
                    m_dest[meta] = int(meta_val)
                case "float":
                    m_dest[meta] = float(meta_val)


def odim_openradar_msgmem(
    odim_content: bytes, schema_file: Path
) -> list[dict[str, Any]]:
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    ret: list[dict[str, Any]] = []
    s3_key_dir = ""
    s3_key_fil = ""
    s3_key_level = []
    s3_key_quan = []

    with open(schema_file, mode="r", encoding="utf-8") as default_schema:
        def_msg = json.load(default_schema)

    fb = io.BytesIO(odim_content)

    odim = h5py.File(fb)

    # datetime
    dt = odim_datetime(odim["what"].attrs["date"], odim["what"].attrs["time"])
    # TODO: import api.metadata_endpoints.py -> datetime_to_iso_string
    def_msg["properties"]["datetime"] = dt.isoformat() + "Z"
    s3_key_dir += datetime.strftime(dt, "%Y/%m/%d/")
    s3_key_fil += datetime.strftime(dt, "%Y%m%dT%H%M")

    # source
    source = get_attr_str(odim["what"], "source")
    if source is None:
        raise ValueError("ODIM file source attribute missing")
    wigos = find_source_type(source, "WIGOS")
    wmo = find_source_type(source, "WMO")
    nod = find_source_type(source, "NOD")
    org = find_source_type(source, "ORG")
    station = find_source_type(source, "PLC")

    if wigos:
        def_msg["properties"]["platform"] = wigos
    else:
        if wmo:
            def_msg["properties"]["platform"] = DEFAULT_WMO + wmo.zfill(5)
        else:
            if nod:
                def_msg["properties"]["platform"] = DEFAULT_WIGOS + nod
            else:
                if org == "247":
                    def_msg["properties"]["platform"] = "0-20010-0-" + "OPERA"
                    def_msg["properties"]["platform_name"] = "OPERA"

    if nod:
        if station:
            def_msg["properties"]["platform_name"] = "[" + nod + "]" + " " + station
        else:
            def_msg["properties"]["platform_name"] = "[" + nod + "]"

    if nod:
        s3_key_dir += str(nod)[:2].upper() + "/" + str(nod) + "/"
        s3_key_fil = str(nod) + "@" + s3_key_fil
    else:
        if org == "247":
            s3_key_dir += "OPERA/"
            s3_key_fil = "OPERA" + "@" + s3_key_fil
            def_msg["properties"]["period_int"] = 300
            def_msg["properties"]["period"] = "PT300S"
        else:
            s3_key_dir += "Unknown/" + def_msg["properties"]["platform"] + "/"
            s3_key_fil = "Unknown" + "@" + s3_key_fil

    obj = get_attr_str(odim["what"], "object")
    form_version = get_attr_str(odim["what"], "version")
    def_msg["properties"]["format"] = "ODIM"
    def_msg["properties"]["radar_meta"]["format_version"] = form_version
    def_msg["properties"]["radar_meta"]["object"] = str(obj)

    if obj == "COMP":
        def_msg["geometry"] = {}
        # def_msg["geometry"]["type"] = "Polygon"
        def_msg["geometry"]["type"] = "Point"
        coords = {}
        coords["lon"] = 0.0
        coords["lat"] = 0.0
        # Set "Central coordinates"
        for corner in ["LL", "LR", "UL", "UR"]:
            for geo_coord in ["lat", "lon"]:
                meta = corner + "_" + geo_coord
                meta_val = get_attr(odim["where"], meta)
                coords[geo_coord] += meta_val
                def_msg["properties"]["radar_meta"][meta] = str(meta_val)

        coords["lat"] /= 4
        coords["lon"] /= 4
        def_msg["properties"]["hamsl"] = 0.0
        # coords["hei"] = 0.0
        def_msg["geometry"]["coordinates"] = coords
        add_str_attrs = ["projdef"]
        set_meta(
            def_msg["properties"]["radar_meta"], odim["where"], add_str_attrs, "str"
        )
        add_int_attrs = ["xsize", "ysize"]
        set_meta(
            def_msg["properties"]["radar_meta"], odim["where"], add_int_attrs, "int"
        )
        add_float_attrs = ["xscale", "yscale"]
        set_meta(
            def_msg["properties"]["radar_meta"], odim["where"], add_float_attrs, "float"
        )
    else:
        if obj in ["PVOL", "SCAN"]:
            def_msg["properties"]["period_int"] = 300
            def_msg["properties"]["period"] = "PT300S"
            def_msg["geometry"] = {}
            def_msg["geometry"]["type"] = "Point"
            coords = {}
            coords["lat"] = get_attr(odim["where"], "lat")
            coords["lon"] = get_attr(odim["where"], "lon")
            # coords["hei"] = get_attr(odim["where"], "height")
            def_msg["geometry"]["coordinates"] = coords
            def_msg["properties"]["hamsl"] = get_attr(odim["where"], "height")

            # what attibutes => radar_meta
            if "how" in odim:
                add_attrs = [
                    "wavelength",
                    "frequency",
                    "beamwidth",
                    "beamwH",
                    "beamwV",
                    "hiprf",
                    "midprf",
                    "lowprf",
                    "antspeed",
                    "pulsewidth",
                ]
                set_meta(
                    def_msg["properties"]["radar_meta"], odim["how"], add_attrs, "float"
                )
                if "beamwidth" in def_msg["properties"]["radar_meta"]:
                    bw = def_msg["properties"]["radar_meta"]["beamwidth"]
                    def_msg["properties"]["radar_meta"]["beamwH"] = bw
                    del def_msg["properties"]["radar_meta"]["beamwidth"]
                if "wavelength" in def_msg["properties"]["radar_meta"]:
                    wl = float(def_msg["properties"]["radar_meta"]["wavelength"])
                    freq = 299792485 / (wl / 100.0)
                    def_msg["properties"]["radar_meta"]["frequency"] = str(freq)
                    del def_msg["properties"]["radar_meta"]["wavelength"]

    s3_key_dir += str(obj) + "/"
    dataset_index = 1
    dataset_key = "dataset" + str(dataset_index)
    level = 0
    ingest_list = []
    while dataset_key + "/what" in odim:
        dataset_msg = copy.deepcopy(def_msg)

        # print("DATASET WHAT: {0}".format(odim[dataset_key + "/what"]))
        # Use dataset startdate, starttime
        st = odim_datetime(
            odim[dataset_key + "/what"].attrs["startdate"],
            odim[dataset_key + "/what"].attrs["starttime"],
        )
        et = odim_datetime(
            odim[dataset_key + "/what"].attrs["enddate"],
            odim[dataset_key + "/what"].attrs["endtime"],
        )
        td = et - st
        period_int = int(td.total_seconds())
        dataset_msg["properties"]["datetime"] = st.isoformat() + "Z"
        dataset_msg["properties"]["period_int"] = period_int
        dataset_msg["properties"]["period"] = "PT" + str(period_int) + "S"

        if dataset_key + "/where" in odim:
            # print("DATASET WHERE: {0}".format(odim[dataset_key + "/where"]))
            elangle = get_attr(odim[dataset_key + "/where"], "elangle")
            if elangle is not None:
                dataset_msg["properties"]["radar_meta"]["elangle"] = float(elangle)

        # if dataset_key + "/how" in odim:
        #     print("DATASET HOW: {0}".format(odim[dataset_key + "/how"]))

        if obj in ["PVOL", "SCAN"]:
            if elangle is not None:
                level = round(elangle, 2)
                if elangle not in s3_key_level:
                    s3_key_level.append(elangle)

            # what attibutes => radar_meta int
            for meta in ["nbins", "nrays", "a1gate"]:
                meta_val = get_attr(odim[dataset_key + "/where"], meta)
                if meta_val is not None:
                    dataset_msg["properties"]["radar_meta"][meta] = int(meta_val)
            # what attibutes => radar_meta float
            for meta in ["rstart", "rscale"]:
                meta_val = get_attr(odim[dataset_key + "/where"], meta)
                if meta_val is not None:
                    dataset_msg["properties"]["radar_meta"][meta] = float(meta_val)

        product = get_attr_str(odim[dataset_key + "/what"], "product")
        if product:
            dataset_msg["properties"]["radar_meta"]["product"] = product
        prodpar = get_attr(odim[dataset_key + "/what"], "prodpar")
        if prodpar is not None:
            dataset_msg["properties"]["radar_meta"]["prodpar"] = str(prodpar)

        if obj == "COMP":
            match product:
                case "CAPPI" | "PCAPPI" | "PPI" | "ETOP" | "EBASE" | "RHI":
                    if prodpar is None:
                        level = 0
                        if 0 not in s3_key_level:
                            s3_key_level.append(0)
                    else:
                        level = prodpar
                        if prodpar not in s3_key_level:
                            s3_key_level.append(prodpar)
                case "VIL":
                    if prodpar is not None:
                        level = prodpar[1]
                        for vp in prodpar:
                            if vp not in s3_key_level:
                                s3_key_level.append(vp)
                case _:
                    if 0 not in s3_key_level:
                        s3_key_level.append(0)

        dataset_msg["properties"]["level"] = level

        data_index = 1
        data_key = "dataset" + str(dataset_index) + "/data" + str(data_index)
        while data_key + "/what" in odim:

            msg = copy.deepcopy(dataset_msg)

            if "quantity" in odim[data_key + "/what"].attrs:
                quantity = (
                    odim[data_key + "/what"]  # pylint: disable=no-member
                    .attrs["quantity"]  # pylint: disable=no-member
                    .decode("utf-8")  # pylint: disable=no-member
                )

                current_ingest = st.isoformat() + "Z_" + str(level) + "_" + quantity
                if current_ingest in ingest_list:
                    # print("Duplicate quantity: {0}, skip: {1} ".format(current_ingest, data_key))
                    pass
                else:
                    if quantity in radar_cf:
                        ingest_list.append(current_ingest)

                        msg["properties"]["content"]["standard_name"] = quantity
                        if quantity not in s3_key_quan:
                            s3_key_quan.append(str(quantity))

                        ret.append(copy.deepcopy(msg))
                    else:
                        # print("WARNING, unknown quantity: {0}, skip: {1}".format(quantity, data_key))
                        pass
            data_index += 1
            data_key = "dataset" + str(dataset_index) + "/data" + str(data_index)
        dataset_index += 1
        dataset_key = "dataset" + str(dataset_index)

    # example S3 key:
    # dkrom-20240912T0355@0.47_0.65_0.96_1.46_2.36_4.47_8.49_9.96_13.0_15.0@DBZH_LDR_PHIDP_RHOHV_TH_VRAD_WRAD_ZDR.h5
    # Where:
    # ODIM Id - Datetime @ elevations(sorted, 2 digits) @ quantities(sorted) .h5
    s3_key_start_delim = "@"
    s3_key_field_delim = "_"
    s3_key_delim = s3_key_start_delim
    s3_key_level.sort()
    for el in s3_key_level:
        s3_key_fil += s3_key_delim + str(round(float(el), 2))
        if s3_key_delim == s3_key_start_delim:
            s3_key_delim = s3_key_field_delim
    s3_key_delim = s3_key_start_delim
    s3_key_quan.sort()
    for qa in s3_key_quan:
        s3_key_fil += s3_key_delim + qa
        if s3_key_delim == s3_key_start_delim:
            s3_key_delim = s3_key_field_delim
    s3_key = s3_key_dir + s3_key_fil + ".h5"

    for msg in ret:
        data_link_href = S3_ENDPOINT_URL
        if len(data_link_href) > 1 and data_link_href[-1] != "/":
            data_link_href += "/"
        data_link_href += S3_BUCKET_NAME + "/" + s3_key
        data_link = default_data_link
        data_link["href"] = data_link_href
        data_link["length"] = len(data_link_href)
        msg["links"].append(data_link)

    # S3 upload + error check

    # Update link
    if DEFAULT_URL:
        for json_str in ret:
            json_str["links"][0]["href"] = DEFAULT_URL
    return ret


def odim2mqtt(odim_file_path: Path, schema_file: Path) -> list[dict[str, Any]]:
    with open(odim_file_path, "rb") as file:
        odim_content = file.read()
    ret_str = odim_openradar_msgmem(odim_content, schema_file)
    return ret_str


def main(filename: Path, schema_file: Path | None = None) -> str:
    if schema_file is None:
        schema_file = test_schema_path

    if os.path.exists(filename):
        msg = odim2mqtt(filename, schema_file)
        output_text = json.dumps(msg, indent=2)
        return output_text

    raise FileNotFoundError(f"File does not exist: {filename}")
