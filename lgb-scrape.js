// 🌐 ORGANIZE (Connect layer — project structure and imports)
// These connect your program to external modules (they are "connections", not remembered data)
const axios = require('axios');            // CONNECT → External Helper (Network)
const fs = require('fs');                  // CONNECT → External Helper (File)
const { parse } = require('json2csv');     // CONNECT → External Helper (Data Conversion)


// 🧠 REMEMBER (Hold layer — internal configs and state initialization)
const BASE_URL = 'https://taxsales.lgbs.com/api/property_sales/';           // HOLD-config → constant endpoint
const DETAILS_URL = uid => `https://taxsales.lgbs.com/api/property_sales/${uid}/`; // HOLD-config → reusable helper function
const params = {                          // HOLD-config → constant query parameters
  in_bbox: '-106.65,25.84,-93.51,36.5',
  sale_type: 'FUTURE SALE',
  limit: 1000,
};


// 👂 LISTEN (Watch layer — entry point that waits or responds)
// The async IIFE "listens" for execution (main trigger)

async function lgbscrape ({url}) {

  // 🧠 REMEMBER (Hold layer — runtime mutable state)
  let allProperties = [];   // HOLD-state → accumulates fetched data
  let offset = 0;           // HOLD-state → tracks pagination progress

  // 👁️ LISTEN → Inspect (debug reflection)
  console.log('📦 Fetching all FUTURE SALE properties...');


  // ⚙️ RUN (Act layer — Flow + Logic using Helpers)
  // FLOW: asynchronous network call
  const firstPage = await axios.get(url, { params: { ...params, offset } }); // ACT (Flow) using Helper: axios

  // LOGIC (calc + txn)
  const total = firstPage.data.count;            // ACT (Logic-let-calc): derive total count
  allProperties.push(...firstPage.data.results); // ACT (Logic-let-txn): mutate state array
  offset += params.limit;                        // ACT (Logic-let-txn): update counter


  // 🔁 RUN → FLOW (loop orchestration)
  while (offset < total) {                                      // ACT (Flow-loop): sequence management
    console.log(`➡️ Fetching offset ${offset} of ${total}`);    // WATCH (Listen-Inspect): progress log

    const nextPage = await axios.get(url, { params: { ...params, offset } }); // ACT (Flow-async) using Helper: axios
    allProperties.push(...nextPage.data.results);               // ACT (Logic-let-txn)
    offset += params.limit;                                     // ACT (Logic-let-txn)
  }

  console.log(`✅ Got ${allProperties.length} properties`);      // WATCH (Listen-Inspect)


  // 🧱 RUN (Act layer — nested controller with Guard / Logic / Flow)
  const detailedResults = [];                                   // HOLD-state
  for (let i = 0; i < allProperties.length; i++) {              // ACT (Flow-loop)
    const prop = allProperties[i];                              // ACT (Logic-let-calc)
    try {                                                // GUARD (protective)
      const detailRes = await axios.get(DETAILS_URL(prop.uid)); // ACT (Flow-async) using Helper: axios
      const d = detailRes.data;                                 // ACT (Logic-let-calc)

      // LOGIC: data shaping
      detailedResults.push({                                    // ACT (Logic-let-txn)
        uid: d.uid,
        county: d.county,
        address: `${d.prop_address_one} ${d.prop_city} ${d.prop_state} ${d.prop_zipcode}`,
        value: d.value,
        minimum_bid: d.minimum_bid,
        status: d.status,
        sale_type: d.sale_type,
        sale_notes: d.sale_notes,
        account_nbr: d.account_nbr,
        cause_nbr: d.cause_nbr,
        coordinates: d.geometry?.coordinates?.join(', ')
      });

      if ((i + 1) % 100 === 0) {                                // GUARD (conditional progress checkpoint)
        console.log(`...fetched ${i + 1} details`);             // WATCH (Listen-Inspect)
      }
    } catch (err) {                                             // GUARD (try/catch)
      console.warn(`❌ Error fetching details for UID ${prop.uid}:`, err.message); // WATCH (Listen-feedback)
    }
  }

  // 🧮 LOGIC (data transformation and filtering)
  const excludedCities = ['DALLAS', 'FORT WORTH', 'HOUSTON', 'SAN ANTONIO', 'AUSTIN']; // HOLD-config
  const filteredResults = detailedResults.filter(p => {         // ACT (Logic-let-calc): derive subset
    const city = (p.address || '').toUpperCase();               // ACT (Logic-let-calc)
    return !excludedCities.some(ex => city.includes(ex));       // GUARD (conditional logic)
  });


  // 🧩 LOGIC + HELPER (data conversion using parse)
  const csv = parse(filteredResults, {                          // ACT (Logic-let-calc): conversion using Helper: parse
    fields: ['uid', 'county', 'address', 'value', 'minimum_bid', 'status',
             'sale_type', 'sale_notes', 'account_nbr', 'cause_nbr', 'coordinates']
  });


  // 💾 FLOW + HELPER (file write side effect)
  fs.writeFileSync('texas-future-sales.csv', csv);              // ACT (Flow-txn): write file using Helper: fs
  console.log(`📄 CSV file saved as texas-future-sales.csv with ${filteredResults.length} rows.`); // WATCH (Listen-Inspect)

} // END → (Listener trigger closes)

lgbscrape({url: BASE_URL});
