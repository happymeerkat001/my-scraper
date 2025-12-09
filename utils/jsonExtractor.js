// JSON Extractor Utility
// Extracts embedded JSON data from HTML pages

export function extractEmbeddedJSON(html, patterns = null) {
  const defaultPatterns = [
    // window.__data = {...}
    /window\.__data\s*=\s*({[\s\S]*?});/,
    // window.__INITIAL_STATE__ = {...}
    /window\.__INITIAL_STATE__\s*=\s*({[\s\S]*?});/,
    // var appData = {...}
    /var\s+appData\s*=\s*({[\s\S]*?});/,
    // <script type="application/json" id="data">...</script>
    /<script[^>]*type=["']application\/json["'][^>]*>([\s\S]*?)<\/script>/,
    // React/Next.js __NEXT_DATA__
    /id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/
  ];

  const patternsToUse = patterns || defaultPatterns;

  for (const pattern of patternsToUse) {
    const match = html.match(pattern);
    if (match) {
      try {
        const jsonStr = match[1].trim();
        return JSON.parse(jsonStr);
      } catch (e) {
        // Failed to parse JSON, continue to next pattern
      }
    }
  }

  return null;
}

export function extractJSONFromScript(html, scriptId) {
  const pattern = new RegExp(`<script[^>]*id=["']${scriptId}["'][^>]*>([\\s\\S]*?)<\\/script>`, 'i');
  const match = html.match(pattern);

  if (match) {
    try {
      return JSON.parse(match[1].trim());
    } catch (e) {
      // Failed to parse JSON
    }
  }

  return null;
}

export function findAllJSONObjects(html) {
  const results = [];

  // Find all script tags with JSON
  const scriptPattern = /<script[^>]*>([\s\S]*?)<\/script>/g;
  let match;

  while ((match = scriptPattern.exec(html)) !== null) {
    const content = match[1].trim();

    // Skip if it's JavaScript code (has function/var/const keywords)
    if (/\b(function|const|let|var|if|for|while)\b/.test(content)) continue;

    // Try to parse as JSON
    try {
      const json = JSON.parse(content);
      results.push({ source: 'script-tag', data: json });
    } catch (e) {
      // Not valid JSON, skip
    }
  }

  // Find variable assignments
  const varPattern = /(?:var|let|const|window\.)\s*(\w+)\s*=\s*({[\s\S]*?});/g;

  while ((match = varPattern.exec(html)) !== null) {
    try {
      const json = JSON.parse(match[2]);
      results.push({ source: `variable:${match[1]}`, data: json });
    } catch (e) {
      // Not valid JSON, skip
    }
  }

  return results;
}
