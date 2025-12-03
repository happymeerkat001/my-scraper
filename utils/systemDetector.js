// System Detector
// Automatically identifies which county records system a website uses

export function detectSystem(html, url = '') {
  const indicators = [
    // PublicSearch.us
    {
      name: 'publicsearch',
      patterns: ['tx.publicsearch.us', 'publicsearch-web'],
      confidence: (html, url) => {
        if (url.includes('tx.publicsearch.us')) return 1.0;
        if (html.includes('publicsearch')) return 0.8;
        return 0;
      }
    },

    // TylerTech
    {
      name: 'tyler',
      patterns: ['tylerhost.net', 'Tyler Technologies', 'selfservice', 'Odyssey'],
      confidence: (html, url) => {
        if (url.includes('tylerhost.net')) return 1.0;
        let score = 0;
        if (html.includes('Tyler Technologies')) score += 0.4;
        if (html.includes('selfservice')) score += 0.3;
        if (html.includes('Odyssey')) score += 0.3;
        return score;
      }
    },

    // Fidlar/AVA
    {
      name: 'fidlar',
      patterns: ['fidlar.com', 'AvaWeb', 'Fidlar Technologies'],
      confidence: (html, url) => {
        if (url.includes('fidlar.com')) return 1.0;
        if (html.includes('AvaWeb')) return 0.9;
        if (html.includes('Fidlar Technologies')) return 0.8;
        return 0;
      }
    },

    // i2i/USLandRecords
    {
      name: 'i2i',
      patterns: ['uslandrecords.com', 'i2i'],
      confidence: (html, url) => {
        if (url.includes('uslandrecords.com')) return 1.0;
        if (html.includes('i2i')) return 0.7;
        return 0;
      }
    },

    // Kofile
    {
      name: 'kofile',
      patterns: ['Kofile', 'kofiletech'],
      confidence: (html, url) => {
        if (url.includes('kofile')) return 1.0;
        if (html.includes('Kofile Technologies')) return 0.9;
        if (html.includes('Kofile')) return 0.7;
        return 0;
      }
    },

    // Granicus
    {
      name: 'granicus',
      patterns: ['Granicus', 'govqa'],
      confidence: (html, url) => {
        if (url.includes('granicus')) return 1.0;
        if (html.includes('Granicus')) return 0.8;
        return 0;
      }
    },

    // Generic HTML with embedded JSON
    {
      name: 'html-json',
      patterns: ['window.__data', '__INITIAL_STATE__', 'application/json'],
      confidence: (html, url) => {
        let score = 0;
        if (html.includes('window.__data')) score += 0.6;
        if (html.includes('__INITIAL_STATE__')) score += 0.6;
        if (html.includes('type="application/json"')) score += 0.4;
        return Math.min(score, 0.9);
      }
    },

    // PDF-only system
    {
      name: 'pdf-only',
      patterns: ['.pdf', 'application/pdf'],
      confidence: (html, url) => {
        if (url.endsWith('.pdf')) return 1.0;
        const pdfLinks = (html.match(/\.pdf/g) || []).length;
        if (pdfLinks > 5) return 0.8;
        if (pdfLinks > 0) return 0.3;
        return 0;
      }
    }
  ];

  const results = indicators.map(indicator => ({
    name: indicator.name,
    confidence: indicator.confidence(html, url)
  })).filter(r => r.confidence > 0);

  // Sort by confidence
  results.sort((a, b) => b.confidence - a.confidence);

  if (results.length === 0) {
    return { system: 'unknown', confidence: 0, alternatives: [] };
  }

  return {
    system: results[0].name,
    confidence: results[0].confidence,
    alternatives: results.slice(1)
  };
}

export function getSystemMetadata(systemName) {
  const metadata = {
    'publicsearch': {
      name: 'PublicSearch.us',
      vendor: 'Unknown',
      rendering: 'client-side',
      requiresBrowser: true,
      hasAPI: false
    },
    'tyler': {
      name: 'TylerTech/Tyler Host',
      vendor: 'Tyler Technologies',
      rendering: 'server-side + jQuery',
      requiresBrowser: true,
      hasAPI: false
    },
    'fidlar': {
      name: 'Fidlar/AVA',
      vendor: 'Fidlar Technologies',
      rendering: 'client-side (Angular)',
      requiresBrowser: true,
      hasAPI: true
    },
    'i2i': {
      name: 'i2i/US Land Records',
      vendor: 'US Land Records',
      rendering: 'mixed',
      requiresBrowser: true,
      hasAPI: false
    },
    'kofile': {
      name: 'Kofile',
      vendor: 'Kofile Technologies',
      rendering: 'mixed',
      requiresBrowser: true,
      hasAPI: false
    },
    'granicus': {
      name: 'Granicus',
      vendor: 'Granicus',
      rendering: 'mixed',
      requiresBrowser: true,
      hasAPI: true
    },
    'html-json': {
      name: 'HTML with JSON',
      vendor: 'Various',
      rendering: 'hybrid',
      requiresBrowser: false,
      hasAPI: false
    },
    'pdf-only': {
      name: 'PDF Only',
      vendor: 'Various',
      rendering: 'static',
      requiresBrowser: false,
      hasAPI: false
    },
    'unknown': {
      name: 'Unknown System',
      vendor: 'Unknown',
      rendering: 'unknown',
      requiresBrowser: true,
      hasAPI: false
    }
  };

  return metadata[systemName] || metadata['unknown'];
}
