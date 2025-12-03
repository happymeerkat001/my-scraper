# Unified Texas Liens Scraper - Driver Architecture

## Overview

The unified scraper uses a **driver registry pattern** to support multiple county record systems. Each system (PublicSearch.us, TylerTech, Fidlar, etc.) has its own driver that implements a standard interface.

## Architecture

```
Texas-Liens-unified.js (Main Scraper)
├── drivers/
│   ├── baseDriver.js      (Interface all drivers must implement)
│   ├── publicsearch.js    (PublicSearch.us - 35 counties, 397 properties)
│   ├── tyler.js           (TylerTech - 5 counties, 181 properties)
│   ├── fidlar.js          (TODO - Fidlar/AVA - 1 county, 76 properties)
│   └── i2i.js             (TODO - i2i/USLandRecords - 1 county, 20 properties)
└── utils/
    ├── systemDetector.js  (Auto-detect which system a website uses)
    └── jsonExtractor.js   (Extract embedded JSON from HTML pages)
```

## Usage

### Run the scraper:
```bash
# Test with 5 properties
env TEST_LIMIT=5 node Texas-Liens-unified.js

# Run on full dataset
node Texas-Liens-unified.js

# Custom input/output files
env INPUT_FILE=custom.csv OUTPUT_FILE=output.csv node Texas-Liens-unified.js
```

### Output:
- Shows which driver is used for each county
- Reports success rate per driver
- Lists unsupported counties that need driver implementation

## Driver Interface

All drivers must extend `BaseDriver` and implement:

```javascript
class MyDriver extends BaseDriver {
  getName()              // Return driver name (e.g., "tyler")
  canHandle(county)      // Check if this driver supports the county
  async search(county, name, options)  // Perform the search
  parseRecords(html)     // Extract records from HTML/JSON
}
```

## Current Drivers

### ✅ PublicSearch Driver (`drivers/publicsearch.js`)
- **System**: tx.publicsearch.us
- **Counties**: 35 counties (ANDERSON, BEE, CAMERON, JEFFERSON, NUECES, SAN PATRICIO, TARRANT, etc.)
- **Properties**: 397 (46.9% of dataset)
- **Status**: Fully implemented and working
- **Method**: Puppeteer + DOM scraping

### ⏳ Tyler Driver (`drivers/tyler.js`)
- **System**: tylerhost.net
- **Counties**: 5 counties (NAVARRO, POLK, VAN ZANDT, LAMAR, KAUFMAN)
- **Properties**: 181 (21.4% of dataset)
- **Status**: Framework implemented, needs result parsing logic
- **Method**: Puppeteer + form submission

### 📝 TODO Drivers

**Fidlar Driver** (High Priority - 76 properties)
- System: ava.fidlar.com
- Counties: GALVESTON
- Method: API calls (Angular SPA)

**i2i Driver** (Low Priority - 20 properties)
- System: uslandrecords.com
- Counties: MARION
- Method: Puppeteer + DOM scraping

## Adding a New Driver

1. **Create driver file** in `drivers/`:
```javascript
// drivers/mydriver.js
import { BaseDriver } from './baseDriver.js';

export class MyDriver extends BaseDriver {
  constructor(config = {}) {
    super(config);
    this.COUNTY_MAP = {
      'MY COUNTY': 'mycounty'
    };
  }

  getName() {
    return 'mydriver';
  }

  canHandle(county) {
    return !!this.COUNTY_MAP[county];
  }

  async search(county, name, options = {}) {
    // Your scraping logic here
    return [];
  }

  parseRecords(html) {
    // Your parsing logic here
    return [];
  }
}
```

2. **Register driver** in `Texas-Liens-unified.js`:
```javascript
import { MyDriver } from './drivers/mydriver.js';

const drivers = {
  'publicsearch': new PublicSearchDriver(),
  'tyler': new TylerDriver(),
  'mydriver': new MyDriver()  // Add your driver
};
```

3. **Test it**:
```bash
env TEST_LIMIT=5 node Texas-Liens-unified.js
```

## Utilities

### System Detector (`utils/systemDetector.js`)
Automatically identifies which system a website uses:
```javascript
import { detectSystem } from './utils/systemDetector.js';

const result = detectSystem(html, url);
// Returns: { system: 'tyler', confidence: 0.9, alternatives: [...] }
```

### JSON Extractor (`utils/jsonExtractor.js`)
Extracts embedded JSON from HTML pages:
```javascript
import { extractEmbeddedJSON } from './utils/jsonExtractor.js';

const data = extractEmbeddedJSON(html);
// Finds patterns like: window.__data = {...}
```

## Coverage Roadmap

| Phase | Drivers | Properties | Coverage |
|-------|---------|------------|----------|
| **Current** | PublicSearch | 397 | 46.9% |
| **Phase 1** | + Tyler | 578 | 68.2% |
| **Phase 2** | + Fidlar + i2i | 674 | 79.6% |
| **Phase 3** | + 12 unknown | 817+ | ~96%+ |

## Testing

Test individual drivers:
```bash
# Test PublicSearch counties
env TEST_LIMIT=5 node Texas-Liens-unified.js

# Test TylerTech counties
env INPUT_FILE=<(grep "NAVARRO\|POLK" texas-future-sales.csv) node Texas-Liens-unified.js
```

## Error Handling

The scraper handles:
- ✅ Unsupported counties (logs them at end)
- ✅ Driver failures (tries all name variations)
- ✅ Browser cleanup (closes on exit/error)
- ✅ Rate limiting (2 second delay between requests)

## Next Steps

1. ✅ Implement PublicSearch driver (COMPLETE)
2. ⏳ Complete Tyler driver result parsing
3. 📝 Implement Fidlar driver (API-based)
4. 📝 Implement i2i driver
5. 📝 Research 12 unknown county systems
6. 📝 Implement drivers for unknown systems

## Files Modified/Created

**New files:**
- `Texas-Liens-unified.js` - Main unified scraper
- `drivers/baseDriver.js` - Driver interface
- `drivers/publicsearch.js` - PublicSearch.us driver (working)
- `drivers/tyler.js` - TylerTech driver (partial)
- `utils/systemDetector.js` - System detection utility
- `utils/jsonExtractor.js` - JSON extraction utility

**Original files** (kept for reference):
- `Texas-Liens-claude.js` - Original PublicSearch-only scraper
- `Texas-Liens-tyler.js` - Original standalone Tyler scraper
