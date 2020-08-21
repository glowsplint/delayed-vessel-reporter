## Delay Report Rewrite

### Overview

The delay report script aims to find the updated_eta and updated_etd of certain vessels provided within "Vessel Delay Tracking.XLSX". This is done by querying a variety of carrier APIs and from a static G2 Schedules Excel document. None of the API interactions use the Rio Tinto credentials so requests cannot be traced back to the RT credentials.

The script is written in a modular approach to increase ease of maintenance and improve code quality. Configurations are stored in a `data` subdirectory. The script expects a `Vessel Delay Tracking.XLSX` file and `g2_filename` (G2 Schedule Excel file) in the same directory.

### Features

1. Avoids detection
   - Uses API calls instead of Selenium which is easily detectable
   - Uses randomised timing for API requests
2. Modular
   - If one component breaks, you can always disable it without affecting the other modules

| Carrier   | Status    | APIs                                             | Cookies required? |
| --------- | --------- | ------------------------------------------------ | ----------------- |
| G2OCEAN   | Completed | -                                                | -                 |
| MSC       | Partial   | GET to countryID API <br> GET to Schedules API   | No <br> Yes       |
| OOCL      |           | GET to locationID API <br> POST to Schedules API | Yes <br>          |
| HAMBURG   |           | POST                                             |                   |
| ANL       |           |                                                  |                   |
| HAPAG     |           |                                                  |                   |
| CMA       |           |                                                  |                   |
| ONE       |           | POST                                             |                   |
| EVERGREEN |           | Selenium - CAPTCHA                               |                   |
| COSCO     |           |                                                  |                   |

### Usage

### To-do

1. Initial cookie extraction
2. Increase maintainability
