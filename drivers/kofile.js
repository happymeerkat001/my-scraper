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
      page = await this.safePageCreate();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      // Step 1: Navigate to login page
      await page.goto(config.baseUrl + 'loginDisplay.action?countyname=LimestoneTX', {
        waitUntil: 'networkidle2',
        timeout: 15000
      });

      // Step 2: Click "Login as Public" button
      const loginButton = await page.$('input.basebold1[value="Login as Public"]').catch(() => null);
      if (loginButton) {
        await loginButton.click();
        await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });
      }

      // Step 3: Accept disclaimer
      const acceptButton = await page.$('input[value="Accept"]').catch(() => null);
      if (acceptButton) {
        await acceptButton.click();
        await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });
      }

      // Step 4: Wait for main page to load
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Check current URL - if we're back at login, there's a session issue
      const currentUrl = await page.url();

      if (currentUrl.includes('login.')) {
        throw new Error('Active session error - browser will be restarted on next search');
      }

      // Look for "bodyframe" iframe which contains the search form at disclaimer.do
      await new Promise(resolve => setTimeout(resolve, 2000));

      let bodyFrame = page.frames().find(f => f.name() === 'bodyframe');
      if (!bodyFrame) {
        throw new Error('Could not find bodyframe iframe');
      }

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
        // Wait for the search form to appear in the bodyframe (up to 10 seconds)
        await page.waitForFunction(
          () => {
            const frame = window.frames['bodyframe'];
            if (!frame || !frame.document) return false;
            const searchInput = frame.document.querySelector('input[textboxname="ALLNAMES"]');
            return !!searchInput;
          },
          { timeout: 10000 }
        ).then(() => true).catch(() => false);

        // Re-acquire the bodyframe reference after navigation
        await new Promise(resolve => setTimeout(resolve, 2000));
        const newBodyFrame = page.frames().find(f => f.name() === 'bodyframe');
        if (newBodyFrame) {
          bodyFrame = newBodyFrame;
        }
      }

      const timestamp = Date.now();

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
            btn.click();
            return { clicked: true, text: btn.textContent?.trim() || btn.value };
          }
        }

        return { clicked: false };
      }).catch(() => ({ clicked: false }));

      if (searchNavClicked.clicked) {
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Re-acquire frame reference after navigation
        const newFrame = page.frames().find(f => f.name() === 'bodyframe');
        if (newFrame) bodyFrame = newFrame;
      }

      // The search form is in a nested iframe within bodyframe
      await new Promise(resolve => setTimeout(resolve, 2000));

      let searchFrame = page.frames().find(f => f.name() === 'dynSearchFrame');
      if (!searchFrame) {
        searchFrame = page.frames().find(f => f.name() === 'searchFrame');
      }

      let formFrame = null;  // Frame containing the form fields
      let submitFrame = null; // Frame containing the submit button

      if (searchFrame) {
        submitFrame = searchFrame; // Submit button is in dynSearchFrame

        // Now look for criteriaframe inside dynSearchFrame
        await new Promise(resolve => setTimeout(resolve, 2000));
        const criteriaFrame = page.frames().find(f => f.name() === 'criteriaframe');
        if (criteriaFrame) {
          formFrame = criteriaFrame; // Form fields are in criteriaframe
          bodyFrame = criteriaFrame; // Use criteriaframe for form operations
        } else {
          formFrame = searchFrame;
          bodyFrame = searchFrame;
        }
      } else {
        formFrame = bodyFrame;
        submitFrame = bodyFrame;
      }

      // Wait for search form to load
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check if we have the search form
      const hasSearchForm = await bodyFrame.evaluate(() => {
        return {
          hasAllNamesInput: !!document.querySelector('input[textboxname="ALLNAMES"]'),
          hasTextboxText: !!document.querySelector('input.textbox-text')
        };
      });

      if (!hasSearchForm.hasAllNamesInput && !hasSearchForm.hasTextboxText) {
        throw new Error('Search form not loaded in bodyframe');
      }

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
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Find and click Done button across all frames
        const allFrames = page.frames();

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

          if (clicked) break;
        }

        // Wait for dialog to close
        await new Promise(resolve => setTimeout(resolve, 1500));

        // NOW select "Both" after dialog is closed
        await bodyFrame.evaluate(() => {
          const bothRadio = document.querySelector('#partyBoth');
          if (bothRadio) {
            bothRadio.checked = true;
            bothRadio.click();

            // Manually trigger the onclick function
            if (typeof changeOptions === 'function') {
              changeOptions();
            }
          }
        }).catch(() => {});
        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      // Set the date filter to 11/23/1980
      await bodyFrame.evaluate(() => {
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
                return;
              } catch(e) {
                // Fall through to manual setting
              }
            }

            // Manual setting
            dateInput.value = '11/23/1980';
            dateInput.dispatchEvent(new Event('input', { bubbles: true }));
            dateInput.dispatchEvent(new Event('change', { bubbles: true }));
            return;
          }
        }
      });

      // For EasyUI textboxes, we need to use their API or find the actual hidden input
      const nameInputFilled = await bodyFrame.evaluate((searchName) => {
        // Look for textbox-text input (visible but readonly)
        const visibleInput = document.querySelector('input.textbox-text:not([readonly])') ||
                             document.querySelector('input.textbox-text');

        if (visibleInput) {
          // Try to use EasyUI API if available
          const inputId = visibleInput.id;
          if (inputId && typeof $ !== 'undefined' && $.fn && $.fn.textbox) {
            try {
              $(`#${inputId}`).textbox('setValue', searchName);
              return { success: true };
            } catch(e) {}
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
              visibleInput.value = searchName;
              visibleInput.dispatchEvent(new Event('change', { bubbles: true }));
              return { success: true };
            }
          }

          // Last resort: remove readonly and set value directly
          visibleInput.removeAttribute('readonly');
          visibleInput.value = searchName;
          visibleInput.dispatchEvent(new Event('input', { bubbles: true }));
          visibleInput.dispatchEvent(new Event('change', { bubbles: true }));
          return { success: true };
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
            return { success: true };
          }
        }

        return { success: false };
      }, name);

      if (!nameInputFilled.success) {
        throw new Error('Could not find name input field with any known selector');
      }

      // Search all frames for the tree checkbox to select "All Document Types"
      for (const frame of page.frames()) {
        const result = await frame.evaluate(() => {
          const treeCheckboxes = Array.from(document.querySelectorAll('.tree-checkbox'));
          const allDocsTreeCheckbox = treeCheckboxes.find(cb => {
            const parentNode = cb.closest('.tree-node');
            const titleSpan = parentNode?.querySelector('.tree-title');
            return titleSpan?.textContent?.trim() === 'All Document Types';
          });

          if (allDocsTreeCheckbox) {
            allDocsTreeCheckbox.click();
            return true;
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
                  return true;
                }
              } catch(e) {}
            }
          }

          return false;
        }).catch(() => false);

        if (result) break;
      }

      // Submit search
      const targetFrame = submitFrame || bodyFrame;
      const submitClicked = await targetFrame.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button, input[type="button"], input[type="submit"], a'));
        const searchBtn = buttons.find(b =>
          b.textContent?.toLowerCase().includes('search') ||
          b.value?.toLowerCase().includes('search') ||
          b.getAttribute('onclick')?.includes('search')
        );

        if (searchBtn) {
          searchBtn.click();
          return true;
        }

        return false;
      }).catch(() => false);

      if (!submitClicked) {
        throw new Error('Could not find submit button');
      }

      // Wait for results to load - look for resultFrame
      await new Promise(resolve => setTimeout(resolve, 5000));

      let resultFrame = page.frames().find(f => f.name() === 'resultFrame');
      if (!resultFrame) {
        // Try fallback frames
        resultFrame = page.frames().find(f =>
          f.name() === 'searchResults' ||
          f.name() === 'resultsFrame'
        );
        if (!resultFrame) {
          resultFrame = bodyFrame; // Last resort: fall back to search frame
        }
      }

      // Look for nested resultListFrame
      if (resultFrame) {
        await new Promise(resolve => setTimeout(resolve, 2000));

        const resultListFrame = page.frames().find(f => f.name() === 'resultListFrame');
        if (resultListFrame) {
          resultFrame = resultListFrame; // Use the nested frame for parsing
        }
      }

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
      for (let i = 0; i < Math.min(datagridRows.length, 3); i++) {
        const row = datagridRows[i];
        const cells = row.querySelectorAll('td');

        const record = this.parseDatagridRow(cells);
        if (record) liens.push(record);
      }

      return liens;
    }

    // Fallback: Look for results table rows
    const rows = document.querySelectorAll('table tbody tr');

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

    return liens;
  }

  parseDatagridRow(cells) {
    if (cells.length < 5) {
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
