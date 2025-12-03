// Kofile CountyFusion Driver
// Handles Limestone County (Kofile CountyFusion system with HTML table scrape)

import { BaseDriver } from './baseDriver.js';

export class KofileDriver extends BaseDriver {
  constructor(config = {}) {
    super(config);

    this.COUNTY_CONFIG = {
      'LIMESTONE COUNTY': {
        baseUrl: 'https://countyfusion10.kofiletech.us/countyweb/',
        searchEndpoint: 'searchCriteriaState.do',
        hasCaptcha: false
      }
    };
  }

  getName() {
    return 'kofile';
  }

  canHandle(county) {
    return !!this.COUNTY_CONFIG[county];
  }

  async search(county, name, options = {}) {
    const config = this.COUNTY_CONFIG[county];
    if (!config) {
      console.log(`⚠️  ${county} not in Kofile system`);
      return [];
    }

    console.log(`🔗 URL: ${config.baseUrl}`);

    let page;
    try {
      await this.initBrowser();
      page = await this.browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      // Step 1: Navigate to login page
      await page.goto(config.baseUrl + 'loginDisplay.action?countyname=LimestoneTX', {
        waitUntil: 'networkidle2',
        timeout: 15000
      });

      // Step 2: Click "Login as Public" button
      const loginButton = await page.$('input.basebold1[value="Login as Public"]').catch(() => null);
      if (loginButton) {
        console.log(`📋 Logging in as public...`);
        await loginButton.click();
        await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });
      }

      // Step 3: Accept disclaimer
      const acceptButton = await page.$('input[value="Accept"]').catch(() => null);
      if (acceptButton) {
        console.log(`📋 Accepting disclaimer...`);
        await acceptButton.click();
        await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });
      }

      // Step 4: Wait for main page to load
      console.log(`🔍 Waiting for main page to load...`);
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Check current URL - if we're back at login, there's a session issue
      const currentUrl = await page.url();
      console.log(`   Current URL: ${currentUrl}`);

      if (currentUrl.includes('login.')) {
        throw new Error('Active session error - browser will be restarted on next search');
      }

      // Look for "bodyframe" iframe which contains the search form at disclaimer.do
      console.log(`🔍 Looking for bodyframe iframe...`);
      await new Promise(resolve => setTimeout(resolve, 2000));

      let bodyFrame = page.frames().find(f => f.name() === 'bodyframe');
      if (!bodyFrame) {
        throw new Error('Could not find bodyframe iframe');
      }

      console.log(`   ✓ Found bodyframe`);

      // The bodyframe may show a disclaimer that needs to be accepted
      await new Promise(resolve => setTimeout(resolve, 2000));
      const disclaimerAccepted = await bodyFrame.evaluate(() => {
        // Check if there's a disclaimer with the executeCommand function
        if (typeof window.executeCommand === 'function') {
          window.executeCommand('Accept');
          return true;
        }
        // Fallback: try clicking the Accept button
        const acceptBtn = document.querySelector('input[value="Accept"]');
        if (acceptBtn) {
          acceptBtn.click();
          return true;
        }
        return false;
      }).catch(() => false);

      if (disclaimerAccepted) {
        console.log(`   📋 Accepted disclaimer in bodyframe, waiting for search form...`);

        // Wait for the search form to appear in the bodyframe (up to 10 seconds)
        const searchFormLoaded = await page.waitForFunction(
          () => {
            const frame = window.frames['bodyframe'];
            if (!frame || !frame.document) return false;
            const searchInput = frame.document.querySelector('input[textboxname="ALLNAMES"]');
            return !!searchInput;
          },
          { timeout: 10000 }
        ).then(() => true).catch(() => false);

        if (!searchFormLoaded) {
          console.log(`   ⚠️  Search form didn't load after accepting disclaimer`);
        } else {
          console.log(`   ✓ Search form loaded`);
        }

        // Re-acquire the bodyframe reference after navigation
        await new Promise(resolve => setTimeout(resolve, 2000));
        const newBodyFrame = page.frames().find(f => f.name() === 'bodyframe');
        if (newBodyFrame) {
          bodyFrame = newBodyFrame;
          console.log(`   ✓ Re-acquired bodyframe reference`);
        }
      } else {
        console.log(`   No disclaimer found in bodyframe`);
      }

      // Step 0: Take screenshot for debugging
      const timestamp = Date.now();
      const screenshotFile = `kofile-after-disclaimer-${timestamp}.png`;
      await page.screenshot({ path: screenshotFile, fullPage: true });
      console.log(`   📸 Screenshot saved to ${screenshotFile}`);

      // Step 2: Check for navigation menu to reach search form
      console.log(`🔍 Looking for search navigation...`);

      const searchNavClicked = await bodyFrame.evaluate(() => {
        // Look for "Search Public Records" in datagrid rows
        const patterns = [
          'Search Public Records',
          'Search Records',
          'Name Search',
          'Document Search'
        ];

        // Check datagrid rows (Kofile uses EasyUI datagrid)
        const rows = Array.from(document.querySelectorAll('.datagrid-row'));
        for (const row of rows) {
          const text = row.textContent?.trim();
          for (const pattern of patterns) {
            if (text && text.includes(pattern)) {
              console.log(`Found datagrid row: "${text}" -> clicking`);
              row.click();
              return { clicked: true, text: text };
            }
          }
        }

        // Check all links
        const links = Array.from(document.querySelectorAll('a'));
        for (const pattern of patterns) {
          const link = links.find(a =>
            a.textContent?.toLowerCase().includes(pattern.toLowerCase())
          );
          if (link && link.offsetParent !== null) { // visible
            console.log(`Found nav link: "${link.textContent?.trim()}" -> clicking`);
            link.click();
            return { clicked: true, text: link.textContent?.trim() };
          }
        }

        // Check buttons
        const buttons = Array.from(document.querySelectorAll('button, input[type="button"]'));
        for (const pattern of patterns) {
          const btn = buttons.find(b =>
            (b.textContent?.toLowerCase().includes(pattern.toLowerCase()) ||
             b.value?.toLowerCase().includes(pattern.toLowerCase()))
          );
          if (btn && btn.offsetParent !== null) { // visible
            console.log(`Found nav button: "${btn.textContent?.trim() || btn.value}" -> clicking`);
            btn.click();
            return { clicked: true, text: btn.textContent?.trim() || btn.value };
          }
        }

        return { clicked: false };
      }).catch(() => ({ clicked: false }));

      if (searchNavClicked.clicked) {
        console.log(`   ✓ Clicked navigation: "${searchNavClicked.text}"`);
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Re-acquire frame reference after navigation
        const newFrame = page.frames().find(f => f.name() === 'bodyframe');
        if (newFrame) bodyFrame = newFrame;
      } else {
        console.log(`   ℹ️  No navigation link needed - search form should be visible`);
      }

      // Step 1: Look for nested search form iframe (dynSearchFrame or searchFrame)
      console.log(`🔍 Looking for nested search iframe...`);

      // The search form is in a nested iframe within bodyframe
      await new Promise(resolve => setTimeout(resolve, 2000));

      let searchFrame = page.frames().find(f => f.name() === 'dynSearchFrame');
      if (!searchFrame) {
        searchFrame = page.frames().find(f => f.name() === 'searchFrame');
      }

      if (searchFrame) {
        console.log(`   ✓ Found nested search frame: ${searchFrame.name()}`);
        bodyFrame = searchFrame; // Use the nested iframe for the rest of the operations
      } else {
        console.log(`   ⚠️  No nested search iframe found - trying bodyframe directly`);
      }

      // Wait for search form to load
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check if we have the search form
      const hasSearchForm = await bodyFrame.evaluate(() => {
        // Check for multiple possible indicators of search form
        return {
          hasAllNamesInput: !!document.querySelector('input[textboxname="ALLNAMES"]'),
          hasTextboxText: !!document.querySelector('input.textbox-text'),
          hasNameInput: !!document.querySelector('input[name="Name"]'),
          hasSearchTypes: !!document.querySelector('[id*="SearchTypes"]'),
          hasRecDateInput: !!document.querySelector('input[name="recDateIDFrom"]'),
          allInputs: Array.from(document.querySelectorAll('input')).slice(0, 10).map(inp => ({
            type: inp.type,
            name: inp.name,
            id: inp.id,
            className: inp.className,
            placeholder: inp.placeholder
          })),
          allLinks: Array.from(document.querySelectorAll('a')).slice(0, 5).map(a => ({
            text: a.textContent?.trim().substring(0, 50),
            href: a.href
          }))
        };
      });

      console.log(`   Search form check:`, JSON.stringify(hasSearchForm, null, 2));

      // Save HTML if search form not found
      if (!hasSearchForm.hasAllNamesInput && !hasSearchForm.hasTextboxText) {
        console.log(`   ⚠️  Search form not detected - saving HTML for analysis`);
        const fs = await import('fs');
        const debugHtml = await bodyFrame.content();
        const htmlFile = `kofile-iframe-nosearch-${timestamp}.html`;
        fs.default.writeFileSync(htmlFile, debugHtml);
        console.log(`   💾 Saved to ${htmlFile}`);

        throw new Error('Search form not loaded in bodyframe - see HTML dump for details');
      }

      // Step 4: Click "All Names" or "All Parties" link to open options dialog
      console.log(`📋 Selecting "All Names" search type...`);

      const dialogOpened = await bodyFrame.evaluate(() => {
        // Look for "All Names", "All Parties", or "Alpha-Numeric Only" link
        const links = Array.from(document.querySelectorAll('a'));
        const searchTypeLink = links.find(a =>
          a.textContent?.includes('All Names') ||
          a.textContent?.includes('All Parties') ||
          a.textContent?.includes('Alpha-Numeric')
        );

        if (searchTypeLink) {
          searchTypeLink.click();
          return { opened: true, text: searchTypeLink.textContent?.trim() };
        }
        return { opened: false };
      }).catch(() => ({ opened: false }));

      if (dialogOpened.opened) {
        console.log(`   ✓ Opened dialog: "${dialogOpened.text}"`);
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Select "Both" in the dialog
        const bothSelected = await bodyFrame.evaluate(() => {
          const bothRadio = document.querySelector('input[value="both"]') ||
                           document.querySelector('input#partyBoth');
          if (bothRadio) {
            bothRadio.checked = true;
            bothRadio.click();

            // Click Done button
            const doneBtn = Array.from(document.querySelectorAll('button, input[type="button"]'))
              .find(b => b.textContent?.toLowerCase().includes('done') ||
                         b.value?.toLowerCase().includes('done'));
            if (doneBtn) {
              doneBtn.click();
              return true;
            }
          }
          return false;
        }).catch(() => false);

        console.log(`   ${bothSelected ? '✓' : '⚠️'} Selected "Both" and closed dialog`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        console.log(`   ℹ️  No "All Names" link found - may already be selected`);
      }

      // Step 6: Set the date filter to 11/23/1980
      console.log(`📅 Setting date filter to 11/23/1980...`);
      const dateSet = await bodyFrame.evaluate(() => {
        const dateInput = document.querySelector('input[name="recDateIDFrom"]');
        if (dateInput) {
          dateInput.value = '11/23/1980';
          // Trigger change event
          dateInput.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
        return false;
      });
      console.log(`   Date set: ${dateSet}`);

      // Step 3: Find and fill the name input with updated selectors
      console.log(`✏️  Looking for name input field...`);

      // Try multiple possible selectors (prioritizing textbox-text from screenshots)
      const nameInputFilled = await bodyFrame.evaluate((searchName) => {
        // Try different possible selectors - textbox-text first per user screenshots
        const selectors = [
          'input.textbox-text',                            // From screenshot - try first!
          'input.validatebox-text',                        // Also visible in screenshot
          'input.textbox-prompt',                          // Third class from screenshot
          'input[textboxname="ALLNAMES"]',                 // Original attempt
          'input.easyui-textbox-input1[textboxname="ALLNAMES"]',
          'input[name="ALLNAMES"]',
          'input[name="Name"]',                            // Alternative name
          '#allNamesID'
        ];

        for (const selector of selectors) {
          const input = document.querySelector(selector);
          if (input) {
            input.value = searchName;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            return { success: true, selector, visible: input.offsetParent !== null };
          }
        }

        return { success: false, selector: null };
      }, name);

      console.log(`   Name input filled: ${JSON.stringify(nameInputFilled)}`);

      if (!nameInputFilled.success) {
        throw new Error('Could not find name input field with any known selector');
      }

      // Step 8: Submit search
      console.log(`🔍 Submitting search...`);

      const submitClicked = await bodyFrame.evaluate(() => {
        // Try multiple submit button selectors
        const selectors = [
          'input[type="submit"]',
          'button[type="submit"]',
          'input[value*="Search"]',
          'button:contains("Search")',
          'a.linkbutton'  // EasyUI link button
        ];

        // Also try finding by text content
        const buttons = Array.from(document.querySelectorAll('button, input[type="button"], a'));
        const searchBtn = buttons.find(b =>
          b.textContent?.toLowerCase().includes('search') ||
          b.value?.toLowerCase().includes('search') ||
          b.getAttribute('onclick')?.includes('search')
        );

        if (searchBtn) {
          console.log(`Found submit button: ${searchBtn.tagName} - clicking`);
          searchBtn.click();
          return { success: true, type: searchBtn.tagName };
        }

        return { success: false, type: null };
      }).catch(() => ({ success: false, type: null }));

      if (!submitClicked.success) {
        throw new Error('Could not find submit button');
      }

      console.log(`   ✓ Submit clicked (${submitClicked.type})`);

      // Wait for results to load in the iframe
      await new Promise(resolve => setTimeout(resolve, 5000));

      // Results may be in a different frame - try to find results frame
      let resultsFrame = page.frames().find(f => f.name() === 'searchResults' || f.name() === 'resultsFrame');
      if (!resultsFrame) {
        resultsFrame = bodyFrame; // Fall back to search frame
      }

      // Get HTML from the results iframe
      const html = await resultsFrame.content();

      // Debug: Save results HTML for analysis
      if (process.env.DEBUG) {
        const fs = await import('fs');
        const debugFile = `kofile-results-${timestamp}.html`;
        fs.default.writeFileSync(debugFile, html);
        console.log(`   💾 Saved results to ${debugFile}`);
      }

      return await this.parseRecords(html);

    } catch (e) {
      console.log(`❌ Error:`, e.message);
      return [];
    } finally {
      if (page) await page.close();
      // CRITICAL: Close browser completely to clear session for next search
      // Kofile doesn't allow concurrent sessions
      if (this.browser) {
        await this.browser.close();
        this.browser = null;
        this.browserStartTime = null;
      }
    }
  }

  async parseRecords(html) {
    const jsdom = await import('jsdom');
    const { JSDOM } = jsdom;
    const dom = new JSDOM(html);
    const document = dom.window.document;

    const liens = [];

    // Look for results table rows
    const rows = document.querySelectorAll('table tbody tr');

    console.log(`   Found ${rows.length} result rows`);

    for (let i = 0; i < Math.min(rows.length, 3); i++) {
      const row = rows[i];
      const cells = row.querySelectorAll('td');

      if (cells.length < 5) {
        continue;
      }

      // Based on screenshot 5, the table has columns:
      // Instrument # | Book/Page | Document Type | Name | Other Name | Recorded Date
      const docNumber = cells[0] ? cells[0].textContent.trim() : '';
      const bookVolumePage = cells[1] ? cells[1].textContent.trim() : '';
      const docType = cells[2] ? cells[2].textContent.trim() : '';
      const grantor = cells[3] ? cells[3].textContent.trim() : '';
      const grantee = cells[4] ? cells[4].textContent.trim() : '';
      const recordedDate = cells[5] ? cells[5].textContent.trim() : '';

      const record = {
        grantor,
        grantee,
        docType,
        recordedDate,
        docNumber,
        bookVolumePage,
        legalDescription: '',
        references: ''
      };

      liens.push(record);
    }

    console.log(`   Extracted ${liens.length} records from results table`);
    return liens;
  }
}
