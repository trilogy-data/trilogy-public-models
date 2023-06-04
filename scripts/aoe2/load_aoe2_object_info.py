
TECH_INFO  = {
    "Town Watch": 8,
    "Heavy Plow": 13,
    "Horse Collar": 14,
    "Loom": 22,
    "Gold Mining": 55,
    "Forging": 67,
    "Iron Casting": 68,
    "Scale Mail Armor": 74,
    "Blast Furnace": 75,
    "Chain Mail Armor": 76,
    "Plate Mail Armor": 77,
    "Plate Barding Armor": 80,
    "Scale Barding Armor": 81,
    "Chain Barding Armor": 82,
    "Ballistics": 93,
    "Elite Skirmisher": 98,
    "Crossbowman": 100,
    "Feudal Age": 101,
    "Castle Age": 102,
    "Imperial Age": 103,
    "Gold Shaft Mining": 182,
    "Pikeman": 197,
    "Fletching": 199,
    "Bodkin Arrow": 200,
    "Double-Bit Axe": 202,
    "Bow Saw": 203,
    "Longsword": 207,
    "Padded Archer Armor": 211,
    "Leather Archer Armor": 212,
    "Wheelbarrow": 213,
    "Squires": 215,
    "Man-at-Arms": 222,
    "Stone Mining": 278,
    "Town Patrol": 280,
    "Eagle Warrior": 384,
    "Hussar": 428,
    "Halberdier": 429,
    "Bloodlines": 435,
    "Parthian Tactics": 436,
    "Thumb Ring": 437,
    "Arson": 602,
    "Supplies": 716,
}

TECH_INFO = {v:k for k,v in TECH_INFO.items()}


def get_dict()->dict:
    import requests
    from bs4 import BeautifulSoup
    url = r'http://userpatch.aiscripters.net/unit.ids.html'

    raw = requests.get(url)

    soup = BeautifulSoup(raw.text
                        , 'html.parser')

    tables = soup.find_all('table')
    print(len(tables))

    def get_flattest_item(item):
        output = []
        if hasattr(item, 'children'):
            for c in item.children:
                output += get_flattest_item(c)
            return output
        return [item.text]

    unit_ids = {}
    for table in tables:
        # print(table)
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            flat = [get_flattest_item(x) for x in cols]
            print(flat)
            if not len(flat)>=3:
                continue
            if flat[-1] == ['\xa0']:
                id = int(flat[-3][0])
                name = flat[-2][0]
                unit_ids[id] = name
    return unit_ids


def transform_to_parquet(input:dict):
    import duckdb
    cmd = '''CREATE TABLE unit_ids (id INTEGER, name VARCHAR(255));'''
    duckdb.sql(cmd)
    con = duckdb.connect(':default:')
    for id, name in input.items():
        cmd = f'''INSERT INTO unit_ids VALUES (?, ?);'''
        con.execute(cmd, [id, name])
    cmd = f"""COPY (select id, name from unit_ids) TO 'unit_ids.parquet' (FORMAT PARQUET);"""
    duckdb.sql(cmd)


if __name__ == "__main__":
    object_ids = {**get_dict(), **TECH_INFO}
    transform_to_parquet(object_ids)