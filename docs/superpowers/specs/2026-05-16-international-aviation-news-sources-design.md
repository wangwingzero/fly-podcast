# International Aviation News Sources Design

## Goal

Refocus the daily digest away from accident-heavy and domestic-leaning coverage toward broad international aviation news. The new source strategy should prioritize major international aviation media, use Reuters and Bloomberg only for major aviation-relevant global events, and keep accidents out of the main digest unless they have clear cross-border operational or regulatory impact.

## Current Problem

The current ingest and ranking pipeline already supports RSS plus web list-page crawling, but source mix and ranking pressure still favor safety and incident streams. That leads to a digest that feels narrow, repetitive, and overly centered on accidents instead of global airline, fleet, regulatory, airport, supply-chain, alliance, and airspace developments.

## Outcome

The digest should read like an international aviation industry and operations briefing:

- Primary coverage: airlines, fleet, orders, deliveries, regulation, MRO, airports, alliances, airspace, fuel, labor, and supply-chain developments
- Secondary coverage: major macro events only when aviation impact is explicit
- Exception-only coverage: accidents, severe incidents, and investigations, retained only when they materially affect global operations, fleets, regulators, or major international carriers

## Source Strategy

### A-tier: industry-first international aviation media

These should become the backbone of the digest:

- FlightGlobal
- Aviation Week / ATW
- AIN Online
- Simple Flying
- ch-aviation

Selection rules:

- Prefer public RSS when available
- If RSS is unavailable or incomplete, use existing list-page crawling
- Reserve Playwright for pages where static fetch cannot reliably discover article links or extract usable article content

### B-tier: macro event supplements

These should not drive the digest, only fill true global-impact gaps:

- Reuters
- Bloomberg

Selection rules:

- Keep only aviation-relevant items with explicit operational, regulatory, airspace, airline, fleet, or supply-chain effect
- Enforce low per-source caps
- Reject general politics, finance, tourism, and lifestyle stories unless aviation consequences are concrete

### Explicit exclusions

The default daily digest source pool should exclude or disable:

- Airline corporate newsrooms and airline marketing pages
- China domestic news sites
- Soft promotional aviation content
- Generic travel lifestyle sites without real aviation signal

## Collection Architecture

### Recommended ingestion order

1. RSS
2. Static web fetch plus HTML parsing
3. Playwright fallback

This keeps the main pipeline cheap and stable. Playwright is a targeted recovery tool, not the primary crawler.

### Why not Playwright-first

Using Playwright as the default fetch path would raise cost, complexity, latency, and anti-bot exposure. It is valuable for dynamic list pages and difficult sites, but the main daily pipeline should remain lightweight and deterministic whenever possible.

### Playwright role

Playwright should be used only for:

- Fixed international aviation news sites with unstable static list-page parsing
- Dynamic article listings that hide links until client-side rendering
- Fallback extraction when standard HTTP fetch fails to surface the real article entry points

Playwright should not be used for broad discovery across arbitrary sites.

## Selection Policy

### Promote by default

The ranking stage should favor:

- Airline strategy and network changes
- Fleet orders, deliveries, retirements, leasing, and OEM developments
- Regulator actions, certification, safety directives, and compliance rules
- Airports, airspace, ATC, slot, and operational disruption with broad impact
- MRO, supply-chain, engine, and manufacturing developments
- Alliances, joint ventures, and major labor developments
- SAF, emissions, and sustainability only when operational or commercial implications are concrete

### Demote by default

The ranking stage should demote:

- Isolated accidents and incidents
- Localized event reports without cross-border significance
- Soft PR, brand content, and promotional launches
- Generic tourism and destination pieces
- Macro headlines without explicit aviation consequences

### Exception rule for accidents

Accidents, severe incidents, and investigation stories should be retained only when one or more of the following is true:

- They trigger major grounding, inspection, or fleet-wide operational action
- They drive regulator intervention across countries or regions
- They materially affect a major international airline, airport, OEM, or engine program
- They produce meaningful airspace, route, airport, maintenance, or supply-chain consequences

Otherwise, they should not form part of the main daily digest.

## Implementation Direction

### Source configuration

Update `config/sources.yaml` so that:

- International industry media are promoted into the default enabled pool
- Airline newsroom sources are removed from the default digest pool
- Reuters and Bloomberg are configured as low-volume supplement sources
- Accident-centric sources remain available only if their output is subject to much stricter ranking gates

### Keyword and scoring policy

Update `config/keywords.yaml` and ranking heuristics so that:

- International industry terms receive stronger positive signal
- Domestic and non-news promotional patterns are more aggressively rejected
- Accident-heavy terms no longer dominate by default
- Macro political or economic stories need explicit aviation linkage before passing

### Ranking logic

Update `src/flying_podcast/stages/rank.py` so that:

- Per-source concentration is tighter for supplement sources
- Accident/investigation items require stronger evidence thresholds
- Industry diversity is encouraged across airlines, OEMs, airports, regulators, and supply chain
- Reuters/Bloomberg stories are kept only when aviation relevance is detected directly in title or summary

### Parsing and fallback

Update parser and ingest behavior so that:

- Stable RSS remains first-class
- Web parsers are added only for selected fixed sites that deserve long-term maintenance
- Playwright fallback is source-scoped, not global

## Testing

Add or update tests to cover:

- Source pool excludes domestic and airline-newsroom defaults
- Accident-only stories are rejected without global-impact signals
- Reuters/Bloomberg stories are rejected when aviation linkage is weak
- Industry media stories across fleets, regulators, airports, and supply chain score correctly
- Per-source caps prevent one site from dominating the digest

## Operational Guardrails

- Keep source definitions explicit and curated; do not fall back to generic web-wide search for routine daily digest generation
- Prefer RSS where possible because it is more stable than scraping full pages
- Treat Playwright as a bounded fallback tool to reduce breakage and anti-bot exposure
- Do not send full raw HTML into the LLM; clean article text before summarization

## Non-Goals

- Building a general-purpose web discovery engine
- Covering domestic Chinese aviation news in the main international digest
- Treating accidents as the default backbone of the digest
- Using airline corporate sites as primary editorial sources
