// Minimal test harness for Polk County production driver
import { PolkDriver } from './drivers/polk.js';

async function testPolkDriver() {
  console.log('=== Polk County Driver Test ===\n');

  const driver = new PolkDriver();

  try {
    // Name format: LAST FIRST (Polk County format)
    const results = await driver.search('POLK COUNTY', 'SMITH JOHN');

    console.log('\n=== RESULTS ===');
    console.log(`Total records returned: ${results.length}`);

    if (results.length > 0) {
      console.log('\nFirst 3 records:');
      results.slice(0, 3).forEach((r, i) => {
        console.log(`\n[${i + 1}]`);
        console.log(`  Grantor: ${r.grantor}`);
        console.log(`  Grantee: ${r.grantee}`);
        console.log(`  Doc Type: ${r.docType}`);
        console.log(`  Doc Number: ${r.docNumber}`);
        console.log(`  Recorded Date: ${r.recordedDate}`);
      });
    }

    console.log('\n>>> GO/NO-GO: GO — Driver works <<<');

  } catch (error) {
    console.error('\n=== ERROR ===');
    console.error(error.message);
    console.error(error.stack);
    console.log('\n>>> GO/NO-GO: NO-GO — Driver failed <<<');
  } finally {
    await driver.cleanup();
  }
}

testPolkDriver();
