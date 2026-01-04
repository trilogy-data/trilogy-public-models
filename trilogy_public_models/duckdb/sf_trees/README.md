# San Francisco Street Tree List

## Overview

This dataset is the official **Street Tree List** maintained by the **San Francisco Public Works Department (DPW)**. It catalogs individual street trees in the city of San Francisco, including **species**, **planting dates**, **locations**, **size metrics**, and other attributes used for urban forest management. The data supports analysis of tree distribution, species diversity, growth, and infrastructure planning. {index=0}

**Source:**  
- City of San Francisco Open Data Portal (DataSF) – *City Infrastructure* category  
- Dataset identifier: `tkzw-k3nq`  
- License: PDDL 1.0 (Public Domain) 

This dataset is updated periodically.


## Key Fields (Typical Columns)

Below is a representative set of fields commonly included. 

| Field Name       | Description |
|------------------|-------------|
| `TreeID`         | Unique identifier for the tree record |
| `qLegalStatus`   | Legal/administrative status of the tree |
| `qSpecies`       | Scientific and common species name |
| `qAddress`       | Address where the tree is planted |
| `SiteOrder`      | Internal ordering for site location |
| `qSiteInfo`      | Contextual site attributes (e.g., median, sidewalk) |
| `PlantType`      | Type of planting (e.g., Tree vs. other flora) |
| `qCaretaker`     | Entity responsible for tree maintenance |
| `qCareAssistant` | Secondary caretaker (if any) |
| `PlantDate`      | Date the tree was planted |
| `DBH`            | Diameter at Breast Height (a size indicator) |
| `PlotSize`       | Plot area where the tree sits |
| `PermitNotes`    | Notes regarding permits or planting conditions |
| `XCoord`, `YCoord` | Internal grid coordinates |
| `Latitude`, `Longitude` | Geographic coordinates for mapping |



## Geographic Context

Each tree record is tied to a **latitude/longitude coordinate**, allowing for spatial analyses such as canopy distribution mapping and proximity studies relative to urban infrastructure.

## Use Cases

- **Urban forestry analysis** — species distribution, age composition, growth patterns  
- **Environmental planning** — tree canopy coverage, biodiversity goals  
- **Maintenance scheduling** — pruning cycles and condition tracking  
- **Community and research** — citizen science, coursework, tree census comparisons  

## Notes and Limitations

- Data may be updated periodically; refer to the dataset’s metadata for the last modified date. 
- Some fields like DBH or planting dates may have `NULL/NA` values where measurements are not available.  
- The dataset reflects **DPW-maintained** street trees—not private-land trees.  
- Species naming conventions may combine scientific and common names in the same field (e.g., “*Platanus x hispanica :: Sycamore: London Plane*”). 

## Additional Resources

- **Official DataSF portal landing page:** https://data.sfgov.org/d/tkzw-k3nq  
- **SF Street Tree Map (interactive):** Provided by SF Public Works’ Bureau of Urban Forestry for tree lookup and details  
- **Urban Forest Plan & Species Recommendations:** City guidance on recommended plantings and urban canopy goals

