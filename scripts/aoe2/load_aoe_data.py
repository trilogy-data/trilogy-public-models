"""Adhoc script to parse an ao2 sql database into parquet"""

import duckdb
from pathlib import Path


column_restriction = {
    "matches": ["id", "map_id", "time", "ladder_id", "patch_id", "patch_number"]
}


def export_match_player_actions(db_name, mods=100):
    for idx in range(0, 2):
        print(idx)
        cmd = f"""COPY (select *,  id%{mods} mod_division 
        from  {db_name}.match_player_actions 
        where id%{mods} = {idx} ) TO 'match_actions' 
        (FORMAT PARQUET,  PARTITION_BY (mod_division), 
        OVERWRITE_OR_IGNORE);"""
        try:
            duckdb.sql(cmd)
        except Exception as e:
            from time import sleep

            sleep(20)
            if "IO Error" in str(e):
                duckdb.sql(cmd)


def export_data(db_name: str, table_name: str, columns: list[str]):
    cols = ", ".join(columns)
    cmd = f"""COPY (select {cols} from  {db_name}.{table_name}) 
    TO '{table_name}.parquet' (FORMAT PARQUET);"""
    duckdb.sql(cmd)


def load_data(input_path: str):
    db_name = Path(input_path).stem
    cmds = [
        """INSTALL sqlite;""",
        """LOAD sqlite;""",
        f"""ATTACH '{input_path}' (TYPE sqlite);""",
    ]
    for cmd in cmds:
        duckdb.sql(cmd)
    # for table_name in ['matches', 'players', 'match_players',]:
    #     list_sql = f"""SELECT * from {db_name}.{table_name} limit 100;"""
    #     results = duckdb.sql(list_sql).fetchall()
    #     for r in results:
    #         print(r)
    #     export_data(db_name, table_name, column_restriction.get(table_name, ['*']))
    export_match_player_actions(db_name)
    # duckdb.sql('PRAGMA show_tables;').show()


def main():
    path = r"C:\Users\ethan\Downloads\aoepulse_db\aoepulse_db\Sep_18_2022_aoepulse.db"
    load_data(path)

    # export_data()


if __name__ == "__main__":
    main()
