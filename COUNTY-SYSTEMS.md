# Texas County Public Records Systems

This document tracks which public records system each county uses.

## System Types

### 1. PublicSearch.us (Supported by current scraper)
**Working counties (7 confirmed with data):**
- ANDERSON COUNTY - anderson.tx.publicsearch.us (80% success)
- BEE COUNTY - bee.tx.publicsearch.us (100% success)
- CAMERON COUNTY - cameron.tx.publicsearch.us (54% success)
- JEFFERSON COUNTY - jefferson.tx.publicsearch.us (21% success)
- NUECES COUNTY - nueces.tx.publicsearch.us (76% success)
- SAN PATRICIO COUNTY - sanpatricio.tx.publicsearch.us (86% success)
- TARRANT COUNTY - tarrant.tx.publicsearch.us (3% success - investigate)

**Untested but in COUNTY_MAP (28 counties):**
- ECTOR, ERATH, FORT BEND, FRIO, GRAYSON, HARRIS, HIDALGO, HUNT, KERR
- LAMPASAS, MIDLAND, MONTGOMERY, POTTER, SMITH, TRAVIS, UPSHUR, UVALDE
- VICTORIA, WALLER, WASHINGTON, WEBB, WHARTON, WICHITA, WILLIAMSON
- WISE, WOOD, YOUNG, ZAPATA

### 2. TylerTech/Tyler Host
**URL Pattern:** `https://{county}countytx-web.tylerhost.net/`

**Verified counties (181 properties total):**
- **NAVARRO COUNTY** - https://navarrocountytx-web.tylerhost.net/web/search/DOCSEARCH144S1 (132 properties)
- **POLK COUNTY** - https://polkcountytx-web.tylerhost.net (27 properties)
- **VAN ZANDT COUNTY** - https://vanzandtcountytx-web.tylerhost.net (9 properties)
- **LAMAR COUNTY** - https://lamarcountytx-web.tylerhost.net (8 properties)
- **KAUFMAN COUNTY** - https://kaufmancountytx-web.tylerhost.net (5 properties)

**Note:** TylerTech is the second-largest system after PublicSearch.us

### 3. Fidlar/AVA
**URL Pattern:** `https://ava.fidlar.com/TX{County}/AvaWeb/#/search`

**Known counties:**
- **GALVESTON COUNTY** - https://ava.fidlar.com/TXGalveston/AvaWeb/#/search (76 properties)
  - Was in COUNTY_MAP but had 0% success (wrong system)

### 4. US Land Records/i2i
**URL Pattern:** `https://i2i.uslandrecords.com/TX/{County}/D/`

**Known counties:**
- **MARION COUNTY** - https://i2i.uslandrecords.com/TX/Marion/D/ (20 properties)

### 5. Unknown Systems (143 properties across 12 counties)
**Counties needing research:**
- JASPER COUNTY (59 properties) - Largest unknown
- RAINS COUNTY (20 properties)
- GREGG COUNTY (17 properties)
- RUSK COUNTY (11 properties)
- LIMESTONE COUNTY (9 properties)
- LLANO COUNTY (8 properties)
- JIM WELLS COUNTY (6 properties)
- HARDIN COUNTY (4 properties)
- SHELBY COUNTY (3 properties)
- HOUSTON COUNTY (2 properties)
- WARD COUNTY (2 properties)
- WILSON COUNTY (2 properties)

## Properties by System

| System | Counties | Properties | % of Total | Status |
|--------|----------|------------|------------|--------|
| PublicSearch.us | 35 | 397 | 46.9% | ✅ Supported |
| TylerTech | 5 | 181 | 21.4% | ❌ Not supported |
| Unknown | 12 | 143 | 16.9% | ❌ Not supported |
| Fidlar/AVA | 1 | 76 | 9.0% | ❌ Not supported |
| US Land Records | 1 | 20 | 2.4% | ❌ Not supported |
| **Removed (bad DNS)** | 6 | 30 | 3.5% | ❌ Failed |
| **TOTAL** | **60** | **847** | **100%** | **11% working** |

## Implementation Priority

### High Priority (181 properties - 21% of dataset)
**TylerTech System** - 5 verified counties
- NAVARRO (132), POLK (27), VAN ZANDT (9), LAMAR (8), KAUFMAN (5)
- URL: `https://navarrocountytx-web.tylerhost.net/web/search/DOCSEARCH144S1`
- **Action needed:** Research TylerTech search interface and create new scraper

### Medium Priority (143 properties - 17% of dataset)
**Unknown Systems** - 12 counties need research
- Priority: JASPER (59), RAINS (20), GREGG (17), RUSK (11)
- **Action needed:** Manual investigation of each county's public records website

### Lower Priority (96 properties - 11% of dataset)
1. **Fidlar/AVA** - GALVESTON (76)
   - Angular SPA with API backend
   - **Action needed:** API endpoint discovery

2. **US Land Records/i2i** - MARION (20)
   - **Action needed:** Research i2i scraping approach

## Roadmap to 100% Coverage

**Current:** 11% success (92/847 properties with PublicSearch.us only)

**With TylerTech:** 32% success (+181 properties = 273/847)
- Requires building TylerTech scraper module

**With all known systems:** 44% success (+277 properties = 369/847)
- Requires 3 scraper modules (PublicSearch, TylerTech, Fidlar, i2i)

**With unknown systems:** ~100% success (+143 properties = 512+/847)
- Requires researching 12 more counties

## Next Steps

1. ✅ Document all county systems (COMPLETE)
2. ⏭️ Research TylerTech interface at https://navarrocountytx-web.tylerhost.net/
3. ⏭️ Build TylerTech scraper module (+21% coverage)
4. ⏭️ Research unknown county websites (JASPER priority)
5. ⏭️ Consider Fidlar/AVA and i2i if time permits
