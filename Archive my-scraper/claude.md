# 🕸️ Local: Scraper

## 🎯 Priorities
- Scraper correctness  
- Isolated, minimal changes  
- Respect driver boundaries  
- Deterministic behavior  

---

## 📝 Todo-Based Workflow (Mandatory)
Use `tasks/todo.md` for non-trivial work:

1. **Context Review**  
   - Inspect only: drivers/, utils/, root scripts, debug_responses/  
   - Identify minimal file set

2. **Write Plan → tasks/todo.md**  
   - Checklist: `[ ] purpose → inputs → outputs → verification`  
   - Keep tasks small and isolated

3. **Get Confirmation** before coding

4. **Execute Tasks**  
   - One-by-one  
   - Add 1-2 sentence summary per task

5. **End-of-Work Review**  
   - Summaries, debugging notes, files touched, follow-up risks

---

## 🕸️ Scraper Rules

**Selector Safety**  
- List/log selectors before use  
- Ask confirmation if brittle

**Driver Isolation**  
- `drivers/*` are stable boundaries  
- No modifications without approval

**Logging**  
- URL fetched, selector checks, array lengths  
- Minimal unless debugging

**Rate Limits**  
- Enforce `RATE_LIMIT_MS = 2000`  
- Conservative retry logic

---

## 🧪 Debugging Workflow
1. Reproduce with same property/county  
2. Identify boundary (driver → utils → logic → DOM → network)  
3. Add temporary logs, remove after fix  
4. Apply smallest patch  
5. Verify: single-case → multi-county → full scrape

---

## 🔐 File Boundaries

**Freely Modify:**  
- utils/, root scripts  

**Require Approval:**  
- drivers/ modifications  
- Architecture changes  
- New dependencies  
- CSV/output format changes  

---

## 📁 Reference Files (Load on Demand)
README.md, README-UNIFIED.md, COUNTY-SYSTEMS.md, drivers/baseDriver.js, utils/, debug_responses/