// polk-phase0-validation.js
// Polk County IDS Phase 0 Validation Script
// Run with: node polk-phase0-validation.js

import puppeteer from 'puppeteer';

const CREDENTIALS = { user: 'ccpublic', pass: 'public' };
// Note: Site may only work on HTTP (not HTTPS) - legacy IDS system
const LOGIN_URL = 'http://polkcountytx.net/nd2/cgindx501.html';
const TEST_NAME = 'SMITH JOHN';  // Full name format per Polk rules (LAST FIRST)

async function validatePolkCounty() {
  console.log('=== Polk County IDS Phase 0 Validation ===\n');
  console.log('Testing GRANTOR field only (per Phase 0 constraint)');
  console.log(`Test name: "${TEST_NAME}" (LAST FIRST format)\n`);

  // Launch with flags to bypass Chrome security interstitials
  // Required because Polk County IDS site triggers Safe Browsing / security blocks
  // in automated contexts, even though it loads fine in manual Chrome sessions.
  // Using system Chrome to match user's working browser environment.
  const browser = await puppeteer.launch({
    headless: false, // Headful for debugging
    ignoreHTTPSErrors: true, // Allow insecure/mixed content
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', // Use system Chrome
    args: [
      '--disable-extensions',                              // No extensions
      '--disable-web-security',                            // Bypass CORS/security checks
      '--disable-features=IsolateOrigins,site-per-process', // Reduce isolation
      '--disable-features=BlockInsecurePrivateNetworkRequests', // Allow private network
      '--disable-features=SafeBrowsingEnhancedProtection', // Disable enhanced protection
      '--disable-site-isolation-trials',                   // Disable isolation trials
      '--allow-running-insecure-content',                  // Allow HTTP on HTTPS pages
      '--disable-client-side-phishing-detection',          // No phishing detection
      '--safebrowsing-disable-auto-update',                // No Safe Browsing updates
      '--disable-component-update',                        // No component updates
      '--no-first-run',                                    // Skip first-run dialogs
      '--no-default-browser-check',                        // Skip default browser check
      '--disable-background-networking',                   // Disable background network requests
    ]
  });
  const page = await browser.newPage();

  try {
    // Step 1: Load login page
    // Using 'domcontentloaded' instead of 'networkidle2' because legacy IDS/CGI
    // systems often keep background connections open (analytics, keep-alive) that
    // never fully settle, causing networkidle2 to timeout indefinitely.
    console.log('1. Loading login page...');
    await page.goto(LOGIN_URL, { waitUntil: 'domcontentloaded', timeout: 45000 });
    console.log('   ✓ DOMContentLoaded fired');

    // Debug: Log current URL and page title
    const pageUrl = page.url();
    const pageTitle = await page.title();
    console.log(`   Current URL: ${pageUrl}`);
    console.log(`   Page title: "${pageTitle}"`);

    // Debug: Log page HTML snippet
    const htmlSnippet = await page.evaluate(() => document.body?.innerHTML?.substring(0, 500) || 'NO BODY');
    console.log(`   HTML preview: ${htmlSnippet.substring(0, 200)}...`);

    // Checkpoint: Verify login form exists
    // Actual field names: S01USR, S01PWD (not USERID/PSSWD as shown in screenshots)
    const userField = await page.$('input[name="S01USR"]');
    const passField = await page.$('input[name="S01PWD"]');
    console.log(`   ✓ Login form found: ${!!userField && !!passField}`);

    // Debug: List all input fields if form not found
    if (!userField || !passField) {
      const allInputs = await page.evaluate(() =>
        Array.from(document.querySelectorAll('input')).map(i => ({ name: i.name, type: i.type, id: i.id }))
      );
      console.log('   Available inputs:', JSON.stringify(allInputs));
      throw new Error('Login form not found');
    }

    // Step 2: Submit credentials
    console.log('2. Entering credentials...');
    await page.type('input[name="S01USR"]', CREDENTIALS.user);
    await page.type('input[name="S01PWD"]', CREDENTIALS.pass);

    // Step 3: Submit login
    console.log('3. Submitting login...');
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }),
      page.click('input[type="submit"], button[type="submit"], input[value="Log In"], input[value="Login"]')
    ]);
    console.log('   ✓ Post-login DOMContentLoaded fired');

    // Checkpoint: Verify login success
    const currentUrl = page.url();
    console.log(`   Current URL: ${currentUrl}`);
    const loginSuccess = currentUrl.includes('CGINDX501.ws');
    console.log(`   ✓ Login success: ${loginSuccess}`);
    if (!loginSuccess) throw new Error('Login failed - still on login page');

    // Wait a bit for any JS to execute
    await new Promise(r => setTimeout(r, 2000));

    // Check for frames
    const frames = page.frames();
    console.log(`   Frames found: ${frames.length}`);
    for (const frame of frames) {
      console.log(`   Frame: ${frame.url()}`);
    }

    // Debug: List all inputs on search page
    const searchInputs = await page.evaluate(() =>
      Array.from(document.querySelectorAll('input, select')).map(i => ({
        tag: i.tagName, name: i.name, type: i.type, id: i.id
      }))
    );
    console.log('   Search page inputs:', JSON.stringify(searchInputs.slice(0, 15)));

    // Debug: Show page HTML
    const searchHtml = await page.evaluate(() => document.body?.innerHTML?.substring(0, 500) || 'NO BODY');
    console.log(`   Search page HTML: ${searchHtml.substring(0, 300)}...`);

    // Checkpoint: Verify search form exists
    // Actual field name: S502GRNTR (IDS naming pattern)
    const grantorField = await page.$('input[name="S502GRNTR"]');
    console.log(`   ✓ GRANTOR field found: ${!!grantorField}`);
    if (!grantorField) throw new Error('Search form not found - check inputs list above');

    // Step 4: Modify form target to prevent popup
    console.log('4. Modifying form target...');
    await page.evaluate(() => {
      const form = document.querySelector('form');
      if (form) form.removeAttribute('target');
    });
    console.log('   ✓ Form target="_blank" removed');

    // Step 5: Enter test name (GRANTOR ONLY - per Polk rules)
    // Actual field name: S502GRNTR
    console.log(`5. Entering test name: "${TEST_NAME}" in GRANTOR field...`);
    await page.type('input[name="S502GRNTR"]', TEST_NAME);

    // Step 6: Submit search
    console.log('6. Submitting search...');
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 45000 }),
      page.click('input[value="Run Report"]')
    ]);
    console.log('   ✓ Search results DOMContentLoaded fired');

    // Checkpoint: Analyze results
    console.log('7. Analyzing results...');
    const html = await page.content();
    const htmlLength = html.length;

    // Look for results table or no-results message
    const hasTable = html.includes('<table') || html.includes('<TABLE');
    const noResults = html.toLowerCase().includes('no record') ||
                      html.toLowerCase().includes('0 record') ||
                      html.toLowerCase().includes('no match');

    console.log(`   HTML length: ${htmlLength} bytes`);
    console.log(`   Contains table: ${hasTable}`);
    console.log(`   No results indicator: ${noResults}`);

    // Try to count result rows if table exists
    const rowCount = await page.evaluate(() => {
      const tables = document.querySelectorAll('table');
      let totalRows = 0;
      tables.forEach(t => {
        const rows = t.querySelectorAll('tr');
        totalRows += rows.length;
      });
      return totalRows;
    });
    console.log(`   Table rows found: ${rowCount}`);

    // Final verdict
    console.log('\n=== VALIDATION RESULTS ===');
    console.log(`Login: ✓ PASS`);
    console.log(`Search form: ✓ PASS`);
    console.log(`Form target fix: ✓ PASS`);
    console.log(`Search submission: ✓ PASS`);
    console.log(`Results captured: ${hasTable || noResults ? '✓ PASS' : '✗ FAIL'}`);

    if (hasTable && rowCount > 1) {
      console.log(`\n>>> GO/NO-GO: GO — Results found (${rowCount} rows). Proceed to implementation <<<`);
    } else if (noResults) {
      console.log('\n>>> GO/NO-GO: GO — "No results" message detected. System working correctly <<<');
    } else {
      console.log('\n>>> GO/NO-GO: UNCERTAIN — Manual inspection required <<<');
    }

    // Keep browser open for manual inspection
    console.log('\nBrowser left open for inspection. Close manually when done.');
    console.log('Check the page to verify results structure for parseRecords() implementation.');

  } catch (error) {
    console.error('\n=== VALIDATION FAILED ===');
    console.error(`Error: ${error.message}`);
    console.log('\n>>> GO/NO-GO: NO-GO — Do not proceed <<<');
    await browser.close();
  }
}

validatePolkCounty();
