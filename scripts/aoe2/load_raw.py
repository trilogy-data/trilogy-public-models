from pathlib import Path
from json import loads

def load_raw(print_civs=False, print_research=False, print_units=False, print_buildings=False):
    json_f = Path(__file__).parent / 'raw_data.json'
    with open(json_f, 'r') as f:
        loaded = loads(f.read())
    
    for key in loaded.keys():
        print(key)
    if print_civs:
        items = list(loaded['civ_names'].items())
        items.sort(key=lambda x: x[1])
        for name, id in items:
            id = int(id)-10270
            print(f"WHEN id={id} THEN '{name}'")
    if print_research:
        items = list(loaded['data']['techs'].items())

        # items.sort(key=lambda x: x[1])
        for id, info in items:
            # print(name, sid)
            # id = int(id)-10270
            name = info['internal_name']
            print(f"WHEN id={id} THEN '{name}'")
    if print_units:
        items = list(loaded['data']['units'].items())

        # items.sort(key=lambda x: x[1])
        for id, info in items:
            # print(name, sid)
            # id = int(id)-10270
            name = info['internal_name']
            print(f"WHEN id={id} THEN '{name}'")
    if print_buildings:
        items = list(loaded['data']['buildings'].items())

        # items.sort(key=lambda x: x[1])
        for id, info in items:
            # print(name, sid)
            # id = int(id)-10270
            name = info['internal_name']
            print(f"WHEN id={id} THEN '{name}'")
if __name__ == "__main__":
    load_raw(print_buildings=True)