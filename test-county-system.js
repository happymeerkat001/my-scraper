// Test County System Detector
// Usage: node test-county-system.js <url>

import puppeteer from 'puppeteer';
import { detectSystem, getSystemMetadata } from './utils/systemDetector.js';
import { extractEmbeddedJSON, findAllJSONObjects } from './utils/jsonExtractor.js';

async function testCountySystem(url) {
  console.log(`\n🔍 Testing: ${url}\n`);

  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();

  console.log('📡 Loading page...');
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

  await new Promise(r => setTimeout(r, 3000));

  const html = await page.content();
  console.log(`✓ Loaded ${html.length} chars\n`);

  // Detect system
  console.log('🔎 Detecting system...');
  const detection = detectSystem(html, url);
  console.log(`   Primary: ${detection.system} (confidence: ${(detection.confidence * 100).toFixed(0)}%)`);

  if (detection.alternatives.length > 0) {
    console.log(`   Alternatives:`);
    detection.alternatives.forEach(alt => {
      console.log(`     - ${alt.name} (${(alt.confidence * 100).toFixed(0)}%)`);
    });
  }

  const metadata = getSystemMetadata(detection.system);
  console.log(`\n📋 System Metadata:`);
  console.log(`   Name: ${metadata.name}`);
  console.log(`   Vendor: ${metadata.vendor}`);
  console.log(`   Rendering: ${metadata.rendering}`);
  console.log(`   Requires Browser: ${metadata.requiresBrowser}`);
  console.log(`   Has API: ${metadata.hasAPI}`);

  // Check for embedded JSON
  console.log(`\n🔍 Searching for embedded JSON...`);
  const embeddedJSON = extractEmbeddedJSON(html);

  if (embeddedJSON) {
    console.log(`✓ Found embedded JSON!`);
    console.log(`   Keys: ${Object.keys(embeddedJSON).slice(0, 10).join(', ')}`);
  } else {
    console.log(`   No embedded JSON found`);
  }

  // Find all JSON objects
  const allJSON = findAllJSONObjects(html);
  if (allJSON.length > 0) {
    console.log(`\n✓ Found ${allJSON.length} JSON object(s):`);
    allJSON.forEach((item, i) => {
      console.log(`   ${i + 1}. Source: ${item.source}`);
      const keys = Object.keys(item.data);
      if (keys.length > 0) {
        console.log(`      Keys: ${keys.slice(0, 5).join(', ')}${keys.length > 5 ? '...' : ''}`);
      }
    });
  }

  // Check for forms
  const forms = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('form')).map(form => ({
      action: form.action,
      method: form.method,
      inputCount: form.querySelectorAll('input, select, textarea').length
    }));
  });

  if (forms.length > 0) {
    console.log(`\n📝 Found ${forms.length} form(s):`);
    forms.forEach((form, i) => {
      console.log(`   ${i + 1}. ${form.method.toUpperCase()} to ${form.action || 'same page'} (${form.inputCount} inputs)`);
    });
  }

  // Check for tables
  const tables = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('table')).map((table, idx) => ({
      index: idx,
      rows: table.querySelectorAll('tr').length,
      headers: Array.from(table.querySelectorAll('th')).map(th => th.textContent.trim()).slice(0, 5)
    }));
  });

  if (tables.length > 0) {
    console.log(`\n📊 Found ${tables.length} table(s):`);
    tables.forEach((table, i) => {
      console.log(`   ${i + 1}. ${table.rows} rows`);
      if (table.headers.length > 0) {
        console.log(`      Headers: ${table.headers.join(', ')}`);
      }
    });
  }

  // Save HTML for inspection
  const fs = await import('fs');
  const filename = `test-${detection.system}-${Date.now()}.html`;
  fs.writeFileSync(filename, html);
  console.log(`\n💾 Saved HTML to: ${filename}`);

  await browser.close();

  console.log(`\n✅ Test complete!\n`);
}

// Get URL from command line
const url = process.argv[2];

if (!url) {
  console.log(`Usage: node test-county-system.js <url>`);
  console.log(`\nExamples:`);
  console.log(`  node test-county-system.js https://navarrocountytx-web.tylerhost.net/web/search/DOCSEARCH144S1`);
  console.log(`  node test-county-system.js https://anderson.tx.publicsearch.us`);
  console.log(`  node test-county-system.js https://ava.fidlar.com/TXGalveston/AvaWeb/`);
  process.exit(1);
}

testCountySystem(url).catch(console.error);
