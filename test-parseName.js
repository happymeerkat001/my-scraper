// Quick test for parseName function
// Copy-paste parseName and dependencies here for testing

// Dependencies from Texas-Liens-unified.js
const RE_VS_SPLIT = /\s+V\.?S\.?\s+/i;
const RE_ET_AL = /\s+ET\s+AL\b.*/i;
const RE_NOISE_LEGAL = /\b(IN\s+REM|ESTATE\s+OF|CONSERVATOR(?:SHIP)?(?:\s+OF)?|GUARDIANSHIP\s+OF|MINOR)\b/gi;
const RE_NOISE_ENTITIES = /\b(UNKNOWN\s+HEIRS?|UNKNOWN\s+SUCCESSORS?|CREDITORS?)\b/gi;
const RE_ALIAS = /\s+(?:A\.?K\.?A\.?|F\.?K\.?A\.?)\s+.*/i;
const RE_TRUSTEE = /,?\s*AS TRUSTEE.*$/i;
const RE_BUSINESS_KEYWORDS = /\b(LLC|INC|CORP|CORPORATION|LP|LLP|LTD|CO|COMPANY|BANK|INVESTMENTS|PROPERTIES|ENTERPRISES|MINISTRIES)\b/i;
const RE_SUFFIX = /^(JR|SR|III|II|IV|V)$/i;
const RE_PARENTHETICAL = /\s*\([^)]*\)\s*/g;

function normalizeCaseStyle(raw) {
  if (!raw) return '';
  return raw
    .toUpperCase()
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .replace(/[^\x20-\x7E]/g, '')
    .trim();
}

function normalizeForDriver(name, driverType) {
  return name;
}

function parseName(rawCaseStyle, driverType = 'default') {
  const caseStyle = normalizeCaseStyle(rawCaseStyle);
  if (!caseStyle) return { original: rawCaseStyle || '', variations: [], reason: 'empty_input' };

  // Split on VS to extract defendant
  const text = caseStyle;
  const parts = text.split(RE_VS_SPLIT);
  if (parts.length < 2) return { original: caseStyle, variations: [], reason: 'no_vs_separator' };

  let defendant = parts[1].trim();

  // Remove ET AL and everything after it
  defendant = defendant.replace(RE_ET_AL, '');

  // Remove noise patterns and normalize defendant text
  defendant = defendant
    .replace(RE_NOISE_LEGAL, '')
    .replace(RE_NOISE_ENTITIES, '')
    .replace(RE_ALIAS, '')
    .replace(RE_TRUSTEE, '')
    .replace(RE_PARENTHETICAL, ' ')
    .normalize('NFKC')
    .replace(/\./g, '')
    .replace(/,/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!defendant) return { original: caseStyle, variations: [], reason: 'empty_defendant' };

  // Check for business entities
  if (RE_BUSINESS_KEYWORDS.test(defendant)) {
    return { original: defendant, variations: [defendant] };
  }

  // Individual name parsing
  const words = defendant.split(/\s+/).filter(w => w && !RE_SUFFIX.test(w));

  if (words.length === 0) return { original: defendant, variations: [], reason: 'only_suffixes' };
  if (words.length === 1) return { original: defendant, variations: [words[0]] };

  // Special case: Two single-letter initials + surname (e.g., "A C MARTIN")
  if (words.length === 3 &&
      words[0].length === 1 &&
      words[1].length === 1 &&
      words[2].length > 1 &&
      /^[A-Z]+$/i.test(words[2])) {
    const lastName = words[2];
    const firstMiddle = `${words[0]} ${words[1]}`;
    const fullName = `${lastName} ${firstMiddle}`;
    return {
      original: defendant,
      variations: [fullName]
    };
  }

  // Input format: FIRST [MIDDLE...] LAST
  // Output format: LAST FIRST [MIDDLE...] (uniform for all non-Polk drivers)
  const lastName = words[words.length - 1];
  const firstMiddle = words.slice(0, -1).join(' ');
  const normalizedName = firstMiddle ? `${lastName} ${firstMiddle}` : lastName;

  const variations = [];

  // All non-Polk drivers use LAST FIRST [MIDDLE...] format
  // No last-name-only fallbacks
  variations.push(normalizedName);

  // Apply driver-specific normalization to variations
  const normalizedVariations = variations.map(v => normalizeForDriver(v, driverType));
  return { original: defendant, variations: normalizedVariations };
}

// Run tests
console.log('Test 1: JOHN SMITH');
console.log(JSON.stringify(parseName('STATE OF TEXAS VS JOHN SMITH'), null, 2));

console.log('\nTest 2: JOHN MICHAEL SMITH');
console.log(JSON.stringify(parseName('STATE OF TEXAS VS JOHN MICHAEL SMITH'), null, 2));

console.log('\nTest 3: A C MARTIN');
console.log(JSON.stringify(parseName('STATE OF TEXAS VS A C MARTIN'), null, 2));

console.log('\nTest 4: J R SMITH');
console.log(JSON.stringify(parseName('STATE OF TEXAS VS J R SMITH'), null, 2));
