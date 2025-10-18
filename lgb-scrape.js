const axios = require('axios');                           // Organize ▸ imports
const fs = require('fs');                                 // Organize ▸ imports

(async () => {                                            // Run ▸ Trigger
  const response = await axios.get('https://taxsales.lgbs.com/api/property_sales/', { // Run ▸ Controller
    params: {                                             // Run ▸ Controller
      in_bbox: '-127.8,-10.8,-65.6,69.2',  // Full US bounding box  // Run ▸ Controller
      sale_type: 'SALE,RESALE,STRUCK OFF,FUTURE SALE',              // Run ▸ Controller
      limit: 100  // Default is 10, you can go higher (up to 1000 depending on API) // Run ▸ Controller
    }
  });

  const data = response.data;                             // Run ▸ Controller
  const results = data.results; // ✅ the actual array    // Run ▸ Controller

  console.log(`Got ${results.length} properties`);        // Run ▸ Controller
  console.log(results.slice(0, 3)); // Just show first 3  // Run ▸ Controller

  // Save to file
  fs.writeFileSync('lgb-results.json', JSON.stringify(results, null, 2)); // Run ▸ Renderer
})();                                                     // Run ▸ Trigger