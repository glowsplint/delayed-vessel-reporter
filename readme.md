## Delay Report Rewrite

### Overview

The delay report script aims to find the updated_eta and updated_etd of certain vessels provided within "Vessel Delay Tracking.XLSX". This is done by querying a variety of carrier APIs and from a static G2 Schedules Excel document.

The script is written in a modular approach to increase ease of maintenance and improve code quality. Configurations are stored in a `data` subdirectory. The script expects a `Vessel Delay Tracking.XLSX` file and `g2_filename` (G2 Schedule Excel file) in the same directory.

### Features

1. Avoids detection
   - Uses API calls instead of Selenium which is easily detectable
   - Uses randomised timing for API requests
2. Modular
   - If one component breaks, you can always disable it without affecting the other modules
3. Smart running
   - Saves progress within the day and can continue on failure

| Carrier   | Status    | Approach                    | Duration |
| --------- | --------- | --------------------------- | -------- |
| G2OCEAN   | Completed | -                           | All      |
| MSC ID    | Completed | GET to countryID API        | -        |
| MSC       | Completed | GET to Schedules API        | 8 (12)   |
| OOCL ID   | Completed | GET to locationID API       | -        |
| OOCL      | Completed | POST to Schedules API       | 12       |
| COSCO     | Completed | POST                        | 12       |
| HAMBURG   | Completed | GET direct                  | 12       |
| ANL       | Completed | GET with pd.read_html       | 12       |
| CMA       | Completed | GET with pd.read_html       | 12       |
| ONE       | Completed | POST                        | 12       |
| HAPAG     | N/A       | Unable - Dynamic JS loading | -        |
| EVERGREEN | N/A       | Unable - CAPTCHA            | -        |

### Learnings

1. Sometimes a POST request can be expressed as a GET request
   - g-captcha covered POST request but not GET request
2. Form submission can be a GET request directly pulling a pre-rendered HTML document instead of XHR
