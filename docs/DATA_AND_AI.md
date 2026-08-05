# Data, Sources, and AI

## Data categories

Depending on configuration, Project V Watchtower can display or organize:

- News and public reporting
- Natural events
- Weather
- Fires
- Economic indicators
- Infrastructure signals
- Maritime and aviation information
- Public cameras and media
- Alerts
- Maps and geospatial records
- User-imported documents
- User-created cases, notes, and spreadsheets
- Public-source research links

## Provider responsibility

Every provider has its own:

- Accuracy
- latency
- coverage
- rate limits
- licensing
- attribution
- authentication
- retention
- privacy policy
- acceptable-use terms

Project V Watchtower cannot guarantee provider continuity or correctness.

## Provenance

Where possible, analysts should preserve:

- Source title
- URL
- retrieval time
- publication time
- excerpt
- coordinates
- provider
- case relationship
- confidence
- uncertainty
- conflicting accounts

A model summary without the original source is not adequate provenance.

## Local AI

Ollama can run models locally, but “local” does not make a model accurate.

Model behavior depends on:

- Model family and size
- Quantization
- prompt
- context length
- supplied sources
- available memory
- runtime configuration

## Analysis Room

The specialist roles create structured perspectives. They may all use the same model and therefore share its biases and blind spots.

Use the roles to organize review:

- Collector: what is present
- Verifier: what is supported
- Timeline: when events occurred
- Contradiction: which accounts conflict
- Geospatial: where claims fit
- Briefing: how the findings can be summarized

Do not count role agreement as multiple independent sources.

## Prompt-injection risk

Imported documents and web text can contain instructions intended for an AI model.

Treat source content as evidence to analyze, not as trusted instructions. Do not allow source text to override system boundaries, reveal secrets, or authorize actions.

## High-stakes use

Do not rely solely on Project V or AI output for:

- Emergency response
- Medical decisions
- Legal conclusions
- Financial trading
- Military decisions
- Personal identification
- Accusations of wrongdoing
- Public safety warnings

Use authoritative sources and qualified human review.
