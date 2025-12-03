import puppeteer from 'puppeteer';
import fs from 'fs';

async function investigateTylerTech() {
  const browser = await puppeteer.launch({ headless: false }); // Open browser to see
  const page = await browser.newPage();

  // Enable request interception to capture API calls
  await page.setRequestInterception(true);
  const requests = [];

  page.on('request', request => {
    requests.push({
      url: request.url(),
      method: request.method(),
      postData: request.postData(),
      headers: request.headers()
    });
    request.continue();
  });

  const responses = [];
  page.on('response', async response => {
    if (response.url().includes('search') || response.url().includes('api')) {
      try {
        const body = await response.text();
        responses.push({
          url: response.url(),
          status: response.status(),
          contentType: response.headers()['content-type'],
          body: body.substring(0, 5000) // First 5000 chars
        });
      } catch (e) {}
    }
  });

  console.log('Loading TylerTech page...');
  await page.goto('https://navarrocountytx-web.tylerhost.net/web/search/DOCSEARCH144S1', {
    waitUntil: 'networkidle2',
    timeout: 30000
  });

  // Wait a bit for page to fully load
  await new Promise(r => setTimeout(r, 3000));

  // Save initial HTML
  const initialHtml = await page.content();
  fs.writeFileSync('/Users/leon/Documents/Code/my-scraper/tyler-initial.html', initialHtml);
  console.log('✓ Saved: tyler-initial.html');

  // Detect framework
  const framework = await page.evaluate(() => {
    return {
      react: !!document.querySelector('[data-reactroot], [data-reactid], #root'),
      angular: !!document.querySelector('[ng-app], [ng-version], [ng-controller]'),
      vue: !!document.querySelector('[data-v-], #app.__vue__'),
      jquery: typeof jQuery !== 'undefined'
    };
  });
  console.log('Framework detection:', framework);

  // Find all forms
  const forms = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('form')).map(form => ({
      action: form.action,
      method: form.method,
      id: form.id,
      className: form.className,
      inputs: Array.from(form.querySelectorAll('input, select, textarea')).map(inp => ({
        name: inp.name,
        type: inp.type,
        id: inp.id,
        placeholder: inp.placeholder,
        required: inp.required,
        value: inp.value
      }))
    }));
  });
  console.log('Forms found:', JSON.stringify(forms, null, 2));

  // Try to find name search field
  const searchInputs = await page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll('input[type="text"], input[type="search"]'));
    return inputs.map(inp => ({
      id: inp.id,
      name: inp.name,
      placeholder: inp.placeholder,
      ariaLabel: inp.getAttribute('aria-label'),
      className: inp.className
    }));
  });
  console.log('Search inputs:', JSON.stringify(searchInputs, null, 2));

  // Try to enter a test name
  try {
    console.log('\nAttempting test search for "PRICE CHARLIE"...');

    const nameSelectors = [
      'input[name*="name"]',
      'input[placeholder*="name"]',
      'input[id*="name"]',
      'input[name*="grantor"]',
      'input[name*="grantee"]',
      '#searchText',
      '.search-input',
      'input[type="text"]'
    ];

    let found = false;
    for (const selector of nameSelectors) {
      try {
        const element = await page.$(selector);
        if (element) {
          await element.type('PRICE CHARLIE');
          console.log(`✓ Entered name in: ${selector}`);
          found = true;
          break;
        }
      } catch (e) {}
    }

    if (!found) {
      console.log('⚠ Could not find name input field automatically');
    }

    // Try to find and click search button
    const buttonSelectors = [
      'button[type="submit"]',
      'input[type="submit"]',
      'button.search-button',
      '.btn-search',
      '#searchButton',
      'button:has-text("Search")'
    ];

    for (const selector of buttonSelectors) {
      try {
        const button = await page.$(selector);
        if (button) {
          await button.click();
          console.log(`✓ Clicked search button: ${selector}`);
          break;
        }
      } catch (e) {}
    }

    // Wait for results
    await new Promise(r => setTimeout(r, 5000));

    // Save results HTML
    const resultsHtml = await page.content();
    fs.writeFileSync('/Users/leon/Documents/Code/my-scraper/tyler-results.html', resultsHtml);
    console.log('✓ Saved: tyler-results.html');

    // Try to find results table
    const tables = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('table')).map((table, idx) => ({
        index: idx,
        id: table.id,
        className: table.className,
        rowCount: table.querySelectorAll('tr').length,
        headers: Array.from(table.querySelectorAll('th')).map(th => th.textContent.trim()),
        firstRowData: Array.from(table.querySelectorAll('tr:nth-child(2) td')).map(td => td.textContent.trim()).slice(0, 5)
      }));
    });
    console.log('Tables found:', JSON.stringify(tables, null, 2));

  } catch (e) {
    console.error('Search test error:', e.message);
  }

  // Save all captured API calls
  fs.writeFileSync('/Users/leon/Documents/Code/my-scraper/tyler-requests.json',
    JSON.stringify(requests, null, 2));
  fs.writeFileSync('/Users/leon/Documents/Code/my-scraper/tyler-responses.json',
    JSON.stringify(responses, null, 2));

  console.log('\n=== Investigation Complete ===');
  console.log('Files saved:');
  console.log('- tyler-initial.html (page structure)');
  console.log('- tyler-results.html (after search)');
  console.log('- tyler-requests.json (all HTTP requests)');
  console.log('- tyler-responses.json (API responses)');

  console.log('\nBrowser will close in 10 seconds...');
  await new Promise(r => setTimeout(r, 10000));

  await browser.close();
}

investigateTylerTech().catch(console.error);
