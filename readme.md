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
   - Saves progress and can continue on failure

| Carrier   | Status    | Approach                                         | Cookies required? |
| --------- | --------- | ------------------------------------------------ | ----------------- |
| G2OCEAN   | Completed | -                                                | -                 |
| MSC       | Completed | GET to countryID API <br> GET to Schedules API   | No <br> Yes       |
| OOCL      | Completed | GET to locationID API <br> POST to Schedules API | No <br> Yes       |
| COSCO     | Completed | POST                                             | No                |
| HAMBURG   | Completed | GET direct                                       | No                |
| ANL       | Completed | GET with pd.read_html                            | No                |
| CMA       | Completed | GET with pd.read_html                            | No                |
| ONE       | Completed | POST                                             | No                |
| HAPAG     | N/A       | Dynamic JS loading prevents successful scraping  | N/A               |
| EVERGREEN | N/A       | Selenium - CAPTCHA prevents successful scraping  | N/A               |

### Usage

### To-do

1. Initial cookie extraction
2. Fix bugs

### Bugs

1. ANL+CMA needs to search entire 6 weeks from today - rewrite as GET request under doc
2. COSCO transhipment not working
3. OOCL transhipment not working
4. hamburg vessels from 2nd week of month onwards not matching - basing on start of month; extend to 6 weeks from today
5. get rid of first_day in all instances, makes it unstable and overworks script

### Sorted

1. ANL+CMA vessels multiple transhipment

### Sailing schedules

1. msc to add 4 weeks out with etd 8 weeks from today
2. ANL needs to search 12 weeks from today
3. COSCO should already be 12 weeks from today
4. ONE should already be 12 weeks worth

### Testing

Get Karen to check the delay sheet for Sep
Write proper walkthrough of features and usage

### Learnings

1. Sometimes a POST request can be expressed as a GET request
   - g-captcha covered POST request but not GET request
2. Form submission can be a GET request directly pulling a pre-rendered HTML document instead of XHR
