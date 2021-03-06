"""
Python code to rewrite string IDs from each application CSV file to integers in new
CSVs. In addition to the str2int conversion, the string fields are cleaned up of
unwanted characters and formatted so that they can be ingested into a Neo4j graph
"""
import os
import pandas as pd
from typing import Dict, Union


def read_with_nulls(filepath: str, skiprows: Union[None, int] = None) -> pd.DataFrame:
    """Read in CSV as a pandas DataFrame and fill in NaNs as empty strings"""
    df = pd.read_csv(filepath, sep=",", skiprows=skiprows).fillna("")
    return df


def lookup_id(id_map: Dict[str, int], key: str) -> int:
    """Return integer ID for a given string PERSID key"""
    return id_map[key]


def clean_app_file(filename: str, rawfile_path: str, outpath: str) -> Dict[str, int]:
    """Convert string IDs to int for apps/services persistent IDs and output to CSV"""
    apps_df = read_with_nulls(os.path.join(rawfile_path, filename))
    persids = [item.strip() for item in list(apps_df["PERSID"])]
    apps_df = apps_df.drop("PERSID", axis=1)
    apps_df.insert(0, "persid_int", apps_df.index + 1)
    apps_df.rename({"persid_int": "PERSID"}, axis=1, inplace=True)
    apps_df.to_csv(os.path.join(outpath, filename), index=False, header=True)
    # Store a map of str2int IDS for apps
    app_id_map = dict(zip(persids, list(apps_df["PERSID"])))
    return app_id_map


def clean_org_file(
    filename: str, rawfile_path: str, outpath: str, app_id_map: Dict[str, int]
) -> None:
    """Convert string IDs to int for organization's app persistent IDs and output to CSV"""
    orgs_df = read_with_nulls(os.path.join(rawfile_path, filename))
    orgs_df["PERSID"] = orgs_df["PERSID"].str.replace("nr:", "")
    orgs_df.insert(
        0, "persid_int", orgs_df["PERSID"].apply(lambda x: lookup_id(app_id_map, x))
    )
    orgs_df = orgs_df.drop("PERSID", axis=1)
    orgs_df.rename({"persid_int": "APP_PERSID"}, axis=1, inplace=True)
    orgs_df.to_csv(os.path.join(outpath, filename), index=False, header=True)


def clean_ahd_file(
    filename: str, rawfile_path: str, outpath: str, app_id_map: Dict[str, int]
) -> None:
    """Convert string IDs to int for AHD hitrate file and output to CSV"""
    ahd_df = read_with_nulls(os.path.join(rawfile_path, filename))
    ahd_df["PERSID"] = ahd_df["PERSID"].str.replace("nr:", "")
    ahd_df.insert(
        0, "persid_int", ahd_df["PERSID"].apply(lambda x: lookup_id(app_id_map, x))
    )
    ahd_df = ahd_df.drop("PERSID", axis=1)
    ahd_df.rename({"persid_int": "APP_PERSID"}, axis=1, inplace=True)
    ahd_df.to_csv(os.path.join(outpath, filename), index=False, header=True)


def clean_os_instances_file(filename: str, rawfile_path: str, outpath: str) -> None:
    """
    Convert string IDs to int for OS instances monthly usage file and output to CSV.
    Note that the PERSIDs in this file are NOT the same as the PERSIDs for the app file,
    hence we restart the numbering from 1.
    """
    os_instances_df = read_with_nulls(os.path.join(rawfile_path, filename))
    # Rename first column as PERSID to fix unicode data corruption
    os_instances_df.rename({u"os_\ufeffPersID": "PERSID"}, axis=1, inplace=True)
    os_instances_df.insert(0, "persid_int", os_instances_df.index + 1)
    persids = [item.strip() for item in list(os_instances_df["PERSID"])]
    os_instances_df = os_instances_df.drop("PERSID", axis=1)
    os_instances_df.rename({"persid_int": "OS_PERSID"}, axis=1, inplace=True)
    os_instances_df.to_csv(os.path.join(outpath, filename), index=False, header=True)
    # Store a map of str2int IDS for OS instances
    os_id_map = dict(zip(persids, list(os_instances_df["OS_PERSID"])))
    return os_id_map


def clean_similarity_connectedcomps_file(
    filename: str, rawfile_path: str, outpath: str, app_id_map: Dict[str, int]
) -> None:
    """
    Clean up and format string IDs in the connected components similarity table and
    output to CSV.
    """
    similarities_df = read_with_nulls(os.path.join(rawfile_path, filename))
    similarities_df["PersID-1"] = similarities_df["PersID-1"].str.replace("nr:", "")
    similarities_df["PersID-2"] = similarities_df["PersID-2"].str.replace("nr:", "")
    similarities_df.insert(
        0,
        "PERSID_1",
        similarities_df["PersID-1"].apply(lambda x: lookup_id(app_id_map, x)),
    )
    similarities_df.insert(
        1,
        "PERSID_2",
        similarities_df["PersID-2"].apply(lambda x: lookup_id(app_id_map, x)),
    )
    similarities_df = similarities_df.drop(["PersID-1", "PersID-2"], axis=1)
    similarities_df.to_csv(os.path.join(outpath, filename), index=False, header=True)


def main() -> None:
    # Clean main business app file with application persistent IDs
    app_id_map = clean_app_file(files["apps"], rawfile_path, outpath)
    # Clean IT organizations file that connects to app PERSIDs & usage data
    clean_org_file(files["orgs"], rawfile_path, outpath, app_id_map)
    # Clean AHD hit-rate file that connects to app PERSIDs
    clean_ahd_file(files["ahds"], rawfile_path, outpath, app_id_map)
    # Clean OS instances monthly usage file and replace string IDs with unique int IDs
    os_id_map = clean_os_instances_file(files["os"], rawfile_path, outpath)
    # Clean connected components similarity file and replace string IDs with unique int IDs
    clean_similarity_connectedcomps_file(
        files["similarity_connectedcomps"], rawfile_path, outpath, app_id_map
    )


if __name__ == "__main__":
    rawfile_path = "graph_data"
    outpath = "graph_data_clean"
    files = {
        "apps": "20210330_cmdb_ci_business_app_V2_noDescription.csv",
        "orgs": "20210401-AccessIT-APPLICATIONS-ORGANIZATIONS-reduced_CMDB_exact_matches.csv",
        "ahds": "20210517-CMDB-AHD-hits.csv",
        "os": "20210701_OS_InstanceMonthyCashOut.csv",
        "similarity_connectedcomps": "20210719_cmdb_similarities_sentencebert_08_threshold_conntected_components.csv",
    }
    os.makedirs(outpath, exist_ok=True)
    main()
