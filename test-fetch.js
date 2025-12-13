import axios from 'axios';

const url = 'https://anderson.tx.publicsearch.us/results?department=RP&keywordSearch=false&limit=50&offset=0&recordedDateRange=18000101%2C20251115&searchOcrText=false&searchType=quickSearch&searchValue=%22a%20c%20martin%22&sort=desc&sortBy=recordedDate';

(async () => {
  try {
    const res = await axios.get(url, { timeout: 10000 });
    const html = res.data;
    
    // Show first 5000 chars
    console.log('=== RESPONSE SNIPPET ===\n');
    console.log(html.slice(0, 5000));
    console.log('\n=== LOOKING FOR TABLE MARKERS ===\n');
    
    // Check for common table patterns
    if (html.includes('<table')) console.log('✓ Contains <table>');
    if (html.includes('<tbody')) console.log('✓ Contains <tbody>');
    if (html.includes('<tr')) console.log('✓ Contains <tr>');
    if (html.includes('class="')) console.log('✓ Uses CSS classes');
    
    // Find all class names in first few rows
    const classMatches = html.match(/class="[^"]+"/g);
    if (classMatches) {
      console.log('\nClasses found:', [...new Set(classMatches)].slice(0, 20));
    }
  } catch (e) {
    console.error('Error:', e.message);
  }
})();
