// Odyssey/Public Portal Driver
// Handles Dallas County (Odyssey system with JSON API)

import { BaseDriver } from './baseDriver.js';

export class OdysseyDriver extends BaseDriver {
  constructor(config = {}) {
    super(config);

    this.COUNTY_CONFIG = {
      'DALLAS COUNTY': {
        url: 'https://courtsportal.dallascounty.org/DALLASPROD/Home/Dashboard',
        apiEndpoint: '/DALLASPROD/api/Search/GetSmartSearchResults',
        hasCaptcha: true
      }
    };
  }

  getName() {
    return 'odyssey';
  }

  canHandle(county) {
    return !!this.COUNTY_CONFIG[county];
  }

  async search(county, name, options = {}) {
    const config = this.COUNTY_CONFIG[county];
    if (!config) {
      console.log(`⚠️  ${county} not in Odyssey system`);
      return [];
    }

    console.log(`🔗 URL: ${config.url}`);
    console.log(`⚠️  CAPTCHA required - manual solving needed`);

    try {
      const page = await this.safePageCreate();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      // Navigate to dashboard
      await page.goto(config.url, { waitUntil: 'networkidle2', timeout: 30000 });

      // Wait for the main search interface to be ready (user may need to solve captcha)
      await page.waitForSelector('#SearchCriteria', { timeout: 60000 }).catch(() => {});

      // Use page.evaluate() to call the API directly
      const results = await page.evaluate(async (apiEndpoint, searchName) => {
        try {
          // Get RequestVerificationToken from page
          const tokenInput = document.querySelector('input[name="__RequestVerificationToken"]');
          const token = tokenInput ? tokenInput.value : '';

          // Build search payload
          const payload = {
            SearchCriteria: {
              DefendantName: searchName,
              CaseType: 'TAX DELINQUENCY'
            }
          };

          const response = await fetch(apiEndpoint, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'RequestVerificationToken': token
            },
            body: JSON.stringify(payload)
          });

          if (!response.ok) {
            return { error: `API returned ${response.status}` };
          }

          const data = await response.json();
          return { data };

        } catch (err) {
          return { error: err.message };
        }
      }, config.apiEndpoint, name);

      await page.close();

      if (results.error) {
        console.log(`❌ API Error: ${results.error}`);
        return [];
      }

      return this.parseRecords(results.data);

    } catch (e) {
      console.log(`❌ Error:`, e.message);
      return [];
    }
  }

  parseRecords(data) {
    if (!data || !Array.isArray(data)) {
      return [];
    }

    const liens = [];

    for (let i = 0; i < Math.min(data.length, 3); i++) {
      const item = data[i];

      const record = {
        grantor: '', // Not provided in case data
        grantee: item.DefendantName || '',
        docType: item.CaseType || '',
        recordedDate: item.FileDate || '',
        docNumber: item.CaseNumber || '',
        bookVolumePage: '',
        legalDescription: '',
        references: item.CaseURL || ''
      };

      liens.push(record);
    }

    return liens;
  }
}
