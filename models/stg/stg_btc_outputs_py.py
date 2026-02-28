import pandas as pd
import simplejson

def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas", "simplejson"])

    df = dbt.ref("stg_btc").to_pandas()

    # parse json list
    df["OUTPUTS"] = df["OUTPUTS"].apply(simplejson.loads)

    # explode list into rows
    df_exploded = df.explode("OUTPUTS").reset_index(drop=True)

    # normalize dict -> columns
    norm = pd.json_normalize(df_exploded["OUTPUTS"])

    # pick keys and rename to match SQL output columns
    df_outputs = norm[["address", "value"]].rename(
        columns={"address": "OUTPUT_ADDRESS", "value": "OUTPUT_VALUE"}
    )

    # IMPORTANT: prevent duplicates if these columns already exist in df_exploded
    for c in ["OUTPUT_ADDRESS", "OUTPUT_VALUE"]:
        if c in df_exploded.columns:
            df_exploded = df_exploded.drop(columns=[c])

    # combine
    df_final = pd.concat([df_exploded.drop(columns=["OUTPUTS"]), df_outputs], axis=1)

    # optional filter (keep consistent with SQL model)
    df_final = df_final[df_final["OUTPUT_ADDRESS"].notnull()]

    # uppercase for snowflake
    df_final.columns = [c.upper() for c in df_final.columns]

    return df_final