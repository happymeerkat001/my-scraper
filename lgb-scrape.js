const axios = require('axios');
const fs = require('fs');

(async () => {
  let allResults = [];
  let url = 'https://taxsales.lgbs.com/api/property_sales/';
  let params = {
    in_bbox: '-127.8,-10.8,-65.6,69.2',
    sale_type: 'SALE,RESALE,STRUCK OFF,FUTURE SALE',
    limit: 100
  };

  while (url) {
    const res = await axios.get(url, { params });
    const data = res.data;

    allResults.push(...data.results);
    url = data.next;
    params = {}; // `next` already includes query params
  }

  console.log(`Fetched ${allResults.length} properties`);
  fs.writeFileSync('lgb-full-results.json', JSON.stringify(allResults, null, 2));
})();