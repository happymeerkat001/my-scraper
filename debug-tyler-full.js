import puppeteer from 'puppeteer';

const url = 'https://kaufmancountytx-web.tylerhost.net/web/search/DOCSEARCH1008S7';

(async () => {
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 15000 });
  
  const html = await page.content();
  console.log(html); // Full HTML
  
  await browser.close();
})();
