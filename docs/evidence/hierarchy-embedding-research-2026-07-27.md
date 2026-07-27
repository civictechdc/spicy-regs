# Recent graph/embedding/LLM approaches — survey 2026-07-27

Blind external research (claude-fable-5 subagent, web only), window
Nov 2025 – Jul 2026, on novel approaches to large-vocabulary concept
tagging and subsumption grading. Full report in the session record;
condensed findings and dispositions here.

## Findings by direction

1. **Hierarchy-aware embedding geometry — strongest match.** The HiT
   lineage (Language Models as Hierarchy Encoders, NeurIPS 2024 →
   OnT, ISWC 2025 → *Hierarchical Retrieval with OOV Queries: SNOMED
   CT*, arXiv 2511.16698, Nov 2025, code released) trains a hyperbolic
   text encoder self-supervised ON THE TAXONOMY ITSELF (no gold
   labels) so free text can be placed in the space and its most
   specific subsumers retrieved geometrically — validated at ~360k
   concepts, comparable to our 513k. Makes broader/narrower a
   geometric function instead of an LLM-judge call. Counterpoint
   (Google, NeurIPS 2025, arXiv 2509.16411): Euclidean dual encoders
   with hierarchy-synthesized training pairs may suffice. Box/Gaussian
   embeddings (TaxoBell WWW 2026, Polaris) are benchmark-only,
   unproven at scale. Preprocessing for messy partial hierarchies:
   LLM-guided hierarchy restructuring (arXiv 2511.20679).
2. **Semantic IDs / "embed clusters" (RQ-VAE, TIGER lineage):** active
   in-window (hyperbolic RQ-VAE at ICML 2026; trie-aware transformers;
   STATIC vectorized constrained decoding) but ENTIRELY recsys/search —
   no paper applies it to entity linking or subject indexing, and all
   need interaction/supervision data we lack. The trainless variant
   (hierarchical k-means over frozen embeddings) solves ANN speed,
   which is not our bottleneck. The idea's nearest literature is
   silent on vocabularies — itself a finding.
3. **GraphRAG generation:** document-graph centric (HippoRAG 2 is the
   live line). Closest to us: H-TechniqueRAG (arXiv 2604.14166, Apr
   2026 — taxonomy injected as retrieval bias for technique
   annotation; structurally our pipeline) and TaSR-RAG. Nobody has
   published Leiden-community summaries over a controlled vocabulary.
4. **LLM-native:** OAEI 2025 shows LLM ontology matching is commodity
   (subsumption prediction = our grading task under another name).
   Judge distillation is maturing: topic-specific small classifiers
   beat prompted LLM judges (SIGIR 2026, arXiv 2510.04633);
   Representation-as-a-Judge (Jan 2026). LLMs4Subjects (SemEval-2025,
   200+ teams, GND authority file): the winning DNB ensemble
   free-generates keywords with LLMs then MAPS them onto the
   vocabulary with an embedder — zero fine-tuning, national-library
   scale. Converges with the production-systems survey's rec #5
   (`candidate-selection-research-2026-07-27.md`).
5. **Legal domain:** MLEB benchmark + Kanon 2 legal embedder (Isaacus,
   Oct 2025) is the credible domain-embedder swap candidate. No
   FourCorners follow-ups surfaced; VersionRAG quiet.

## Prototype dispositions (gated, in order)

1. **DNB-style free-keyword → vocabulary-mapping channel.** Zero
   training, pinned models suffice, validated at GND scale, and both
   independent research passes converged on it. Adopt if union
   candidate recall@12 on gold improves ≥5 points over the current
   channels.
2. **HiT-lineage hyperbolic subsumption scorer**, trained
   self-supervised on our own broader/narrower edges (partial
   hierarchy; restructuring preprocessing available). A separate
   scorer — retrieval models stay pinned. Adopt if its geometric
   exact/broader/narrower calls agree with the LLM judges on gold +
   silver pairs at ≥ the judges' own 31/35 self-consistency.
3. **Judge distillation into a small classifier** over frozen-embedding
   features, once silver grades reach ~1–2k pairs. Adopt at held-out
   agreement ≥85% with the frontier judge; replaces most judge calls,
   fitting the machine-first attestation posture.

Deliberately skipped: semantic-ID generative retrieval (no
vocabulary-domain validation, data-hungry), box embeddings (unstable,
unproven at 500k).
