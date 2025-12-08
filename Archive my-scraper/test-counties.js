import https from 'https';

const COUNTIES = [
  'anderson', 'angelina', 'bee', 'cameron', 'ector', 'elpaso',
  'erath', 'fortbend', 'frio', 'galveston', 'grayson', 'harris',
  'hidalgo', 'hunt', 'jefferson', 'kerr', 'lampasas', 'maverick',
  'mclennan', 'midland', 'montgomery', 'nueces', 'potter', 'sanpatricio',
  'smith', 'tarrant', 'travis', 'upshur', 'uvalde', 'victoria',
  'waller', 'washington', 'webb', 'wharton', 'wichita', 'williamson',
  'wise', 'wood', 'young', 'zavala', 'zapata'
];

async function testCounty(subdomain) {
  return new Promise((resolve) => {
    const req = https.request({
      hostname: `${subdomain}.tx.publicsearch.us`,
      path: '/',
      method: 'HEAD',
      timeout: 5000
    }, (res) => {
      resolve({ subdomain, status: res.statusCode, works: true });
    });

    req.on('error', (err) => {
      resolve({ subdomain, error: err.code, works: false });
    });

    req.on('timeout', () => {
      req.destroy();
      resolve({ subdomain, error: 'TIMEOUT', works: false });
    });

    req.end();
  });
}

async function main() {
  console.log('Testing all county subdomains...\n');

  const working = [];
  const failing = [];

  for (const county of COUNTIES) {
    const result = await testCounty(county);

    if (result.works) {
      working.push(county);
      console.log(`✅ ${county.padEnd(15)} - HTTP ${result.status}`);
    } else {
      failing.push(county);
      console.log(`❌ ${county.padEnd(15)} - ${result.error}`);
    }

    // Small delay to avoid hammering
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  console.log(`\n📊 Summary:`);
  console.log(`Working: ${working.length}/${COUNTIES.length}`);
  console.log(`Failing: ${failing.length}/${COUNTIES.length}`);
  console.log(`\n✅ Working counties:\n${working.join(', ')}`);
  console.log(`\n❌ Failing counties:\n${failing.join(', ')}`);
}

main();
