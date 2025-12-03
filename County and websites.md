# DALLAS COUNTY (Odyssey / Public Portal API)
URL: https://courtsportal.dallascounty.org/DALLASPROD/Home/Dashboard

Captcha: YES (Google reCAPTCHA)
Requires Puppeteer: YES

Dallas DOES expose a JSON API:
  SmartSearchResults?
  SmartSearchResults2?
  /DALLASPROD/api/Search/GetSmartSearchResults
These return clean JSON containing:
  - CaseNumber
  - FileDate
  - CaseType (TAX DELINQUENCY)
  - LocationName (court)
  - DefendantName
  - CaseURL
  - CaseStatus

Cannot call API directly with fetch():
  Requires:
    - ASP.NET_SessionId cookie
    - RequestVerificationToken
    - CAPTCHA completion

Scraping method:
  1. Puppeteer loads page and solves captcha
  2. Extract cookies + verification token
  3. Use page.evaluate() to call API
  4. Receive JSON results directly

Selectors (if scraping HTML fallback):
  tr.dxgvDataRow
  td[data-label="File Date"]
  td[data-label="Type"]
  td[data-label="Case Number"]
  td[data-label="Style"]

Key selectors:
  tr.dxgvDataRow
  td[data-label="Type"]        → document / case type (e.g. TAX DELINQUENCY)
  td[data-label="File Date"]   → file date
  td[data-label="Case Number"] → case / cause number
  td[data-label="Style"]       → parties / style of case


# KAUFMAN COUNTY (TylerHost – SCRAPEABLE)
URL: https://kaufmancountytx-web.tylerhost.net/web/search/DOCSEARCH1008S7
Rendering: Server-side HTML (SSR), no public JSON API
Captcha: NONE
Scraping method: fetch + cheerio (no Puppeteer required)

DOM structure per result:
  <div id="searchRowDOCxxxx" class="...">
    <div class="selfServiceSearchRowLeft">   (checkbox etc. – ignore)
    <div class="selfServiceSearchRowRight">  (ALL useful lien data)
  </div>

Example:
  <div id="searchRowDOC1007S22" ...>
    <div class="selfServiceSearchRowRight">
      <ul class="ss-searchResultUL five-line-block">
        <li class="searchResultDocument">
          2022-0031608 • B: OPR V: 7769 P: 65 • RELEASE OF LIEN
        </li>
        <li>Recording Date <b>08/17/2022 08:13 AM</b></li>
        <li>Grantor <b>VELPERMONT GROUP LLC</b></li>
        <li>Grantee <b>SANTILLAN LAURA REVELES</b></li>
        <li>Legal Description <b>R SOWELL SUR ACRES: 2.0</b></li>
      </ul>
    </div>
  </div>

Key selectors (browser):
  #searchRowDOC1007S22 > div.selfServiceSearchRowRight  → single row
  .selfServiceSearchRowRight                             → all rows

Key selectors (cheerio in Node):
  $(".selfServiceSearchRowRight")                        → iterate rows
  $(row).find(".searchResultDocument").text().trim()     → doc number + type line
  $(row).find("li:contains('Recording Date') b").text().trim()     → recording date
  $(row).find("li:contains('Grantor') b").text().trim()            → grantor
  $(row).find("li:contains('Grantee') b").text().trim()            → grantee
  $(row).find("li:contains('Legal Description') b").text().trim()  → legal description

  # LAMAR COUNTY (TylerHost – SCRAPEABLE)
URL: https://lamarcountytx-web.tylerhost.net/web/search/DOCSEARCH1307S1

Vendor: Tyler Technologies – Self-Service Web (same as Kaufman County)
Rendering: Server-side HTML (SSR)
API: NONE (XHR only returns filter metadata, not results)
Captcha: YES – on disclaimer page ("I’m not a robot")
Scraping method: fetch + cheerio AFTER passing disclaimer OR Puppeteer for full automation

Process Flow:
  1. Hit disclaimer page:
        https://lamarcountytx-web.tylerhost.net/web/user/disclaimer
     Must click:
        "I'm not a robot" (reCAPTCHA)
        "I Accept"
  2. After acceptance, user is redirected to search form:
        https://lamarcountytx-web.tylerhost.net/web/action/ACTIONGROUP1307S1
  3. Official Public Record Search page:
        https://lamarcountytx-web.tylerhost.net/web/search/DOCSEARCH1307S1
  4. Perform name search → results load via SSR in full HTML

MAIN RESULT BLOCK (SCRAPE THIS):
  <div class="selfServiceSearchRowRight">
    <ul class="ss-searchResultUL five-line-block">
      <li class="searchResultDocument">
         {DocNumber} • B: {Book} V: {Volume} P: {Page} • {DocType}
      </li>
      <li>Recording Date <b>{Date}</b></li>
      <li>Grantor <b>{Grantor}</b></li>
      <li>Grantee <b>{Grantee}</b></li>
      <li>Legal Description <b>{Legal}</b></li>
    </ul>
  </div>

TylerHost DOM pattern:
  Each result row is wrapped in:
    <div id="searchRowDOCxxxx" ... >
       <div class="selfServiceSearchRowLeft">   ← ignore
       <div class="selfServiceSearchRowRight">  ← your data
    </div>

Key selectors (browser):
  #searchRowDOCxxxx > div.selfServiceSearchRowRight
  .selfServiceSearchRowRight

Key selectors (cheerio):
  $(".selfServiceSearchRowRight")
  $(row).find(".searchResultDocument").text().trim()      → doc summary line
  $(row).find("li:contains('Recording Date') b").text()   → recording date
  $(row).find("li:contains('Grantor') b").text()          → grantor
  $(row).find("li:contains('Grantee') b").text()          → grantee
  $(row).find("li:contains('Legal Description') b").text() → legal description

Search formatting:
  Individual Names: LAST FIRST (e.g. "Smith James")
  Organization Names: full literal (e.g. "Texas Bank")

Pagination:
  Pagination uses:
    DOCSEARCH1307S1?page=2
  Scrape until no results.

Scraping requirements:
  OPTION 1 — Puppeteer full automation:
      ✓ solve captcha
      ✓ click "I Accept"
      ✓ perform name search
      ✓ extract HTML
  OPTION 2 — Manual session cookie capture:
      ✓ manually accept disclaimer once
      ✓ reuse session cookie for fetch() + cheerio

# LIMESTONE COUNTY (Kofile CountyFusion – HTML TABLE SCRAPE)

Base URL:
  https://countyfusion10.kofiletech.us/countyweb/

Vendor:
  Kofile CountyFusion (Java .do endpoints, GovOS branding)

Captcha:
  None (standard disclaimer only)

Rendering:
  Server-side HTML (SSR) – results in an HTML <table>, no JSON API.

Key endpoints (not JSON, just form handlers):
  searchCriteriaState.do       → handles search POST
  getInstrumentCategories.do   → loads instrument type list (ignore)
  getSortOrder.do              → loads sort options (ignore)

Result DOM structure:
  <table class="datagrid-btable">
    <tbody>
      <tr id="datagrid-row-r1-2-0" class="datagrid-row ...">
        <!-- col 1 (index 0) -->
        <td field="1">
          <div class="datagrid-cell datagrid-cell-c1-1">
            <a>2021-0001646</a>               ← Instrument # / document number
          </div>
        </td>

        <!-- ... other columns ... -->

        <!-- col 5 (index 4) -->
        <td field="5">
          <div class="datagrid-cell datagrid-cell-c1-5">
            SHERIFF DEED                      ← Instrument type
          </div>
        </td>

        <!-- ... other columns ... -->

        <!-- col 10 (index 9) -->
        <td field="10">
          <div class="datagrid-cell datagrid-cell-c1-10">
            04/16/2021                        ← Recording date
          </div>
        </td>
      </tr>
      <tr id="datagrid-row-r1-2-1" class="datagrid-row datagrid-row-alt">…</tr>
      ...
    </tbody>
  </table>

Generic selectors (browser / cheerio):

  Rows:
    "tr.datagrid-row"

  Cells per row:
    row.querySelectorAll("td .datagrid-cell")
    // index-based:
      cells[0]  → Instrument #
      cells[4]  → Doc type (SHERIFF DEED, AFFIDAVIT, DEED, etc.)
      cells[9]  → Recording date (mm/dd/yyyy)

  Cheerio version:
    $("tr.datagrid-row").each((i, row) => {
      const cells = $(row).find("td .datagrid-cell");
      const docNumber     = cells.eq(0).text().trim();
      const docType       = cells.eq(4).text().trim();
      const recordingDate = cells.eq(9).text().trim();
    });

Notes:
  - Chrome’s JSPaths like
      document.querySelector("#datagrid-row-r1-2-0 > td > table > tbody > tr > td:nth-child(10) > div")
    are useful for discovery but too brittle; use `.datagrid-row` + cell index instead.
  - All usable data is in this table; XHR calls do not return the row data as JSON.




----DONE----APPEND REST

  # McLENNAN COUNTY (TylerHost – HTML block parser)

Base URL:
  https://mclennancountytx-web.tylerhost.net/web/search/

Vendor:
  Tyler Technologies “Self-Service Web”

Rendering:
  All data is delivered inside HTML. NO JSON API is exposed.

Row structure:
  <li class="selfServiceSearchRow" id="searchRowDOC#########">
      <div class="selfServiceSearchRowLeft"> … icons … </div>
      <div class="selfServiceSearchRowRight">
          <h1>2024040682 • DEED</h1>
          <ul class="selfServiceSearchResultColumn">
              <li><b>Recording Date</b> 10/29/2024 01:28 PM</li>
              <li><b>Grantor</b> ROBISON JERRY BOYD</li>
              <li><b>Grantee</b> McLENNAN COUNTY</li>
              <li><b>Legal Description</b> SUBDIVISION: SHADY SHORES ESTATES …</li>
          </ul>
      </div>
  </li>

Selectors:
  All results:
    "li.selfServiceSearchRow .selfServiceSearchRowRight"

  Document header (doc number + type):
    ".selfServiceSearchRowRight h1"

  Recording date:
    ".selfServiceSearchRowRight li:nth-child(1)"

  Grantor:
    ".selfServiceSearchRowRight li:nth-child(2)"

  Grantee:
    ".selfServiceSearchRowRight li:nth-child(3)"

  Legal description:
    ".selfServiceSearchRowRight li:nth-child(4)"

Notes:
  - “Copy JS path” is disabled in TylerHost because components run inside a Shadow DOM.
  - Do NOT use full XPath—breaks instantly.
  - HTML blocks are consistent across all TylerHost counties.

  # NAVARRO COUNTY (TylerHost – HTML SCRAPER)
URL:
  https://navarrocountytx-web.tylerhost.net/web/search/DOCSEARCH144S1

Vendor:
  Tyler Technologies “Self-Service Web”

Behavior:
  - Entire results page is server-rendered HTML (SSR)
  - Sorting is handled server-side via POST hidden fields
  - URL does NOT change after sorting
  - No API endpoints exist for document search
  - JSPath is not available because of Shadow DOM

Result Row Structure:
  <li class="selfServiceSearchRow" id="searchRowDOC###">
    <div class="selfServiceSearchRowRight">
       <h1>DOCNUMBER • DOCTYPE</h1>
       <ul class="selfServiceSearchResultColumn">
           <li>Recording Date …</li>
           <li>Grantor …</li>
           <li>Grantee …</li>
           <li>Legal Description …</li>
       </ul>
    </div>
  </li>

Selectors:
  All rows:
    "li.selfServiceSearchRow"

  Header (doc number + type):
    ".selfServiceSearchRowRight h1"

  Recording date:
    "li:contains('Recording Date')"

  Grantor:
    "li:contains('Grantor')"

  Grantee:
    "li:contains('Grantee')"

  Legal:
    "li:contains('Legal Description')"

Notes:
  - Same structure as Kaufman, McLennan, Anderson, etc.
  - Use POST to apply sorting or page changes.
  - HTML parsing is the only way to extract data.


  # TARRANT COUNTY (Kofile Public Search – HTML Table)
URL Example:
  https://tarrant.tx.publicsearch.us/results?department=RP&...

Vendor:
  Kofile Technologies (publicsearch.us)

Rendering:
  - Fully visible HTML table
  - Each `<tr>` is one document
  - Columns use consistent classes: col-5 (Doc Type), col-6 (Recorded Date)
  - Supports JSPath normally (NOT Shadow DOM)

DOM Structure:
  <table>
     <thead>...</thead>
     <tbody>
         <tr>
             <td class="col-5"><span>DEED OF TRUST</span></td>
             <td class="col-6"><span>11/5/2014</span></td>
         </tr>
         ...
     </tbody>
  </table>

Selectors:
  All rows:
    "table tbody tr"

  Doc Type:
    "td.col-5 span"

  Recorded Date:
    "td.col-6 span"

Cheerio (Node.js):
  $("table tbody tr").each((i, row) => {
      const docType = $(row).find("td.col-5 span").text().trim();
      const recorded = $(row).find("td.col-6 span").text().trim();
  });

Notes:
  - Much simpler than TylerHost.
  - Supports sorting via query parameters (offset, limit, dateRange).
  - Very stable HTML DOM structure.

  