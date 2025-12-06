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

      let formFrame = null;  // Frame containing the form fields
      let submitFrame = null; // Frame containing the submit button

      if (searchFrame) {
        console.log(`   ✓ Found nested search frame: ${searchFrame.name()}`);
        submitFrame = searchFrame; // Submit button is in dynSearchFrame

        // Now look for criteriaframe inside dynSearchFrame
        await new Promise(resolve => setTimeout(resolve, 2000));
        const criteriaFrame = page.frames().find(f => f.name() === 'criteriaframe');
        if (criteriaFrame) {
          console.log(`   ✓ Found criteriaframe (nested inside search frame)`);
          formFrame = criteriaFrame; // Form fields are in criteriaframe
          bodyFrame = criteriaFrame; // Use criteriaframe for form operations
        } else {
          console.log(`   ℹ️  No criteriaframe found, using ${searchFrame.name()} for both form and submit`);
          formFrame = searchFrame;
          bodyFrame = searchFrame;
        }
      } else {
        console.log(`   ⚠️  No nested search iframe found - trying bodyframe directly`);
        formFrame = bodyFrame;
        submitFrame = bodyFrame;
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

        // First, just close the dialog by clicking Done (don't select "Both" yet)
        console.log(`   🔘 Closing dialog first...`);

        // Find and click Done button across all frames
        const allFrames = page.frames();
        let doneClicked = false;

        for (const frame of allFrames) {
          const clicked = await frame.evaluate(() => {
            const doneBtn = document.querySelector('.dialog-button a') ||
                           document.querySelector('.dialog-button .l-btn') ||
                           Array.from(document.querySelectorAll('a, button')).find(b =>
                             b.textContent?.trim() === 'Done'
                           );
            if (doneBtn) {
              doneBtn.click();
              return true;
            }
            return false;
          }).catch(() => false);

          if (clicked) {
            console.log(`   ✓ Clicked Done button in frame: ${frame.name() || 'main'}`);
            doneClicked = true;
            break;
          }
        }

        if (!doneClicked) {
          console.log(`   ⚠️  Done button not found, trying Escape...`);
          await bodyFrame.evaluate(() => {
            document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27 }));
          });
        }

        // Wait for dialog to close
        await new Promise(resolve => setTimeout(resolve, 1500));

        // NOW select "Both" after dialog is closed
        console.log(`   🔘 Selecting "Both" party type...`);
        const bothSelected = await bodyFrame.evaluate(() => {
          const bothRadio = document.querySelector('#partyBoth');
          if (bothRadio) {
            bothRadio.checked = true;
            bothRadio.click();

            // Manually trigger the onclick function
            if (typeof changeOptions === 'function') {
              changeOptions();
            }

            return { success: true, checked: bothRadio.checked };
          }
          return { success: false };
        }).catch(e => ({ success: false, error: e.message }));

        console.log(`   ${bothSelected.success ? '✓' : '⚠️'} Set party type to Both:`, JSON.stringify(bothSelected));
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        console.log(`   ℹ️  No "All Names" link found - may already be selected`);
      }

      // Step 6: Set the date filter to 11/23/1980
      console.log(`📅 Setting date filter to 11/23/1980...`);
      const dateSet = await bodyFrame.evaluate(() => {
        // Try multiple possible selectors for the "from" date field
        const selectors = [
          'input[name="recDateIDFrom"]',
          'input[name="fromDate"]',
          'input[id*="DateFrom"]',
          'input[placeholder*="From"]',
          'input.easyui-datebox',
          'input.datebox-f'
        ];

        for (const selector of selectors) {
          const dateInput = document.querySelector(selector);
          if (dateInput) {
            // Try EasyUI datebox API first if available
            const inputId = dateInput.id;
            if (inputId && typeof $ !== 'undefined' && $.fn && $.fn.datebox) {
              try {
                $(`#${inputId}`).datebox('setValue', '11/23/1980');
                return { success: true, method: 'easyui-datebox', selector };
              } catch(e) {
                // Fall through to manual setting
              }
            }

            // Manual setting
            dateInput.value = '11/23/1980';
            dateInput.dispatchEvent(new Event('input', { bubbles: true }));
            dateInput.dispatchEvent(new Event('change', { bubbles: true }));
            return { success: true, method: 'direct', selector };
          }
        }
        return { success: false, selectorsChecked: selectors.length };
      });
      console.log(`   Date set:`, JSON.stringify(dateSet));

      // Step 3: Find and fill the name input with EasyUI textbox handling
      console.log(`✏️  Looking for name input field...`);

      // For EasyUI textboxes, we need to use their API or find the actual hidden input
      const nameInputFilled = await bodyFrame.evaluate((searchName) => {
        // First, try to find the EasyUI textbox by looking for the wrapper span
        // and finding the associated input

        // Look for textbox-text input (visible but readonly)
        const visibleInput = document.querySelector('input.textbox-text:not([readonly])') ||
                             document.querySelector('input.textbox-text');

        if (visibleInput) {
          // Try to use EasyUI API if available
          const inputId = visibleInput.id;
          if (inputId && typeof $ !== 'undefined' && $.fn && $.fn.textbox) {
            try {
              $(`#${inputId}`).textbox('setValue', searchName);
              return { success: true, method: 'easyui-api', selector: `#${inputId}` };
            } catch(e) {
              console.log('EasyUI API failed:', e.message);
            }
          }

          // Find the parent span and look for hidden input with actual name attribute
          let parent = visibleInput.parentElement;
          while (parent && !parent.classList.contains('textbox')) {
            parent = parent.parentElement;
          }

          if (parent) {
            const hiddenInput = parent.querySelector('input[type="hidden"].textbox-value');
            if (hiddenInput) {
              hiddenInput.value = searchName;
              // Also set visible input for display
              visibleInput.value = searchName;
              visibleInput.dispatchEvent(new Event('change', { bubbles: true }));
              return { success: true, method: 'hidden-input', name: hiddenInput.name };
            }
          }

          // Last resort: remove readonly and set value directly
          visibleInput.removeAttribute('readonly');
          visibleInput.value = searchName;
          visibleInput.dispatchEvent(new Event('input', { bubbles: true }));
          visibleInput.dispatchEvent(new Event('change', { bubbles: true }));
          return { success: true, method: 'direct-readonly-removed', selector: visibleInput.className };
        }

        // Fallback: try other selectors
        const selectors = [
          'input[textboxname="ALLNAMES"]',
          'input[name="ALLNAMES"]',
          'input[name="Name"]',
          '#allNamesID'
        ];

        for (const selector of selectors) {
          const input = document.querySelector(selector);
          if (input) {
            input.value = searchName;
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
            return { success: true, method: 'fallback', selector };
          }
        }

        return { success: false, method: 'none' };
      }, name);

      console.log(`   Name input filled: ${JSON.stringify(nameInputFilled)}`);

      if (!nameInputFilled.success) {
        throw new Error('Could not find name input field with any known selector');
      }

      // Step 7: Select "All Document Types" using EasyUI tree checkbox
      console.log(`📄 Selecting "All Document Types"...`);

      // Search all frames for the tree checkbox
      let docTypesSet = { success: false };
      for (const frame of page.frames()) {
        docTypesSet = await frame.evaluate(() => {
          // The Document Types is an EasyUI tree component with custom checkboxes
          const treeCheckboxes = Array.from(document.querySelectorAll('.tree-checkbox'));
          const allDocsTreeCheckbox = treeCheckboxes.find(cb => {
            const parentNode = cb.closest('.tree-node');
            const titleSpan = parentNode?.querySelector('.tree-title');
            return titleSpan?.textContent?.trim() === 'All Document Types';
          });

          if (allDocsTreeCheckbox) {
            allDocsTreeCheckbox.click();
            return { success: true, method: 'tree-checkbox', checkboxes: treeCheckboxes.length };
          }

          // Try EasyUI tree API
          if (typeof $ !== 'undefined' && $.fn && $.fn.tree) {
            const treeEl = document.querySelector('#instTree, .easyui-tree, [id*="Tree"]');
            if (treeEl && treeEl.id) {
              try {
                const tree = $(`#${treeEl.id}`);
                const root = tree.tree('getRoot');
                if (root) {
                  tree.tree('check', root.target);
                  return { success: true, method: 'easyui-api', treeId: treeEl.id };
                }
              } catch(e) {}
            }
          }

          return {
            success: false,
            treeCheckboxes: treeCheckboxes.length,
            titles: treeCheckboxes.slice(0, 2).map(cb =>
              cb.closest('.tree-node')?.querySelector('.tree-title')?.textContent?.trim()
            )
          };
        }).catch(() => ({ success: false }));

        if (docTypesSet.success) {
          console.log(`   ✓ Found Document Types tree in frame: ${frame.name() || 'main'}`);
          break;
        }
      }

      console.log(`   Document types:`, JSON.stringify(docTypesSet));

      // Step 8: Submit search (using submitFrame, not formFrame)
      console.log(`🔍 Submitting search...`);

      const targetFrame = submitFrame || bodyFrame;
      const submitClicked = await targetFrame.evaluate(() => {
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

      // Wait for results to load - look for resultFrame
      await new Promise(resolve => setTimeout(resolve, 5000));

      let resultFrame = page.frames().find(f => f.name() === 'resultFrame');
      if (!resultFrame) {
        console.log(`   ⚠️  No resultFrame found, trying fallback frames...`);
        // Try fallback frames
        resultFrame = page.frames().find(f =>
          f.name() === 'searchResults' ||
          f.name() === 'resultsFrame'
        );
        if (!resultFrame) {
          resultFrame = bodyFrame; // Last resort: fall back to search frame
        }
      } else {
        console.log(`   ✓ Found resultFrame`);
      }

      // Look for nested resultListFrame
      if (resultFrame) {
        console.log(`   🔍 Looking for nested resultListFrame...`);
        await new Promise(resolve => setTimeout(resolve, 2000));

        const resultListFrame = page.frames().find(f => f.name() === 'resultListFrame');
        if (resultListFrame) {
          console.log(`   ✓ Found resultListFrame (nested)`);
          resultFrame = resultListFrame; // Use the nested frame for parsing
        } else {
          console.log(`   ℹ️  No resultListFrame found, using resultFrame directly`);
        }
      }

      // Wait for and detect datagrid rows
      console.log(`   🔍 Checking for datagrid rows...`);
      const hasDatagrid = await resultFrame.evaluate(() => {
        const rows = document.querySelectorAll('[id^="datagrid-row"]');
        return rows.length > 0 ? rows.length : 0;
      }).catch(() => 0);

      console.log(`   ${hasDatagrid > 0 ? '✓' : '⚠️'} Datagrid rows found: ${hasDatagrid}`);

      // Get HTML from the results iframe
      const html = await resultFrame.content();

      // Debug: Save results HTML, criteria, screenshot, and row samples
      if (process.env.DEBUG) {
        const fs = await import('fs');
        const timestamp = Date.now();

        // Save criteria (search form HTML from bodyFrame)
        const criteriaHtml = await bodyFrame.content();
        const criteriaFile = `debug_responses/kofile-criteria-${timestamp}.html`;
        fs.default.writeFileSync(criteriaFile, criteriaHtml);

        // Save result frame HTML
        const resultFile = `debug_responses/kofile-results-${timestamp}.html`;
        fs.default.writeFileSync(resultFile, html);

        // Take screenshot
        const screenshotFile = `debug_responses/kofile-results-${timestamp}.png`;
        await page.screenshot({
          path: screenshotFile,
          fullPage: true
        });

        // Log first few datagrid rows if found
        const rowSamples = await resultFrame.evaluate(() => {
          const rows = Array.from(document.querySelectorAll('[id^="datagrid-row"]'));
          return rows.slice(0, 3).map(row => ({
            id: row.id,
            html: row.outerHTML.substring(0, 500)
          }));
        }).catch(() => []);

        if (rowSamples.length > 0) {
          console.log(`   📋 Sample datagrid rows:`, JSON.stringify(rowSamples, null, 2));
        } else {
          console.log(`   ⚠️  No datagrid rows found for sampling`);
        }

        console.log(`   💾 Saved DEBUG artifacts:`);
        console.log(`      - Criteria: ${criteriaFile}`);
        console.log(`      - Results: ${resultFile}`);
        console.log(`      - Screenshot: ${screenshotFile}`);
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

    // Try datagrid rows first (Kofile EasyUI structure)
    const datagridRows = document.querySelectorAll('[id^="datagrid-row"]');

    if (datagridRows.length > 0) {
      console.log(`   Found ${datagridRows.length} datagrid rows`);

      for (let i = 0; i < Math.min(datagridRows.length, 3); i++) {
        const row = datagridRows[i];
        const cells = row.querySelectorAll('td');

        const record = this.parseDatagridRow(cells);
        if (record) liens.push(record);
      }

      console.log(`   Extracted ${liens.length} records from datagrid`);
      return liens;
    }

    // Fallback: Look for results table rows
    const rows = document.querySelectorAll('table tbody tr');

    console.log(`   Found ${rows.length} table result rows (fallback)`);

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

  parseDatagridRow(cells) {
    if (cells.length < 5) {
      console.log(`   ⚠️  Skipping row with only ${cells.length} cells`);
      return null;
    }

    // Expected columns (from Kofile systems):
    // Instrument # | Book/Page | Document Type | Name | Other Name | Recorded Date
    const docNumber = cells[0]?.textContent?.trim() || '';
    const bookVolumePage = cells[1]?.textContent?.trim() || '';
    const docType = cells[2]?.textContent?.trim() || '';
    const grantor = cells[3]?.textContent?.trim() || '';
    const grantee = cells[4]?.textContent?.trim() || '';
    const recordedDate = cells[5]?.textContent?.trim() || '';

    return {
      grantor,
      grantee,
      docType,
      recordedDate,
      docNumber,
      bookVolumePage,
      legalDescription: '',
      references: ''
    };
  }
}
