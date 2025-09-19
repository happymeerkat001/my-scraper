const puppeteer = require('puppeteer');

(async () => {
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();

  await page.goto('https://taxsales.lgbs.com/map?lat=39.576604&lon=-96.721782&zoom=4&offset=0&ordering=precinct,sale_nbr,uid&sale_type=SALE,RESALE,STRUCK%20OFF,FUTURE%20SALE', {
    waitUntil: 'networkidle2'
  });

  // Wait for items to load
  await page.waitForSelector('.result-body');

  // Extract items
  const results = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll('.result-body > div'));
    return rows.map(row => {
      return {
        address: row.querySelector('.ng-binding')?.textContent?.trim() || '',
        details: row.textContent.trim()
      };
    });
  });

  console.log('Total items:', results.length);
  console.log(results.slice(0, 5)); // Preview first 5

  await browser.close();
})();