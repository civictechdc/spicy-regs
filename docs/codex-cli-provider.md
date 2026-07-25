# Bare Codex CLI provider

Spicy Regs can run the relation-exclusion diagnostic through the locally
authenticated Codex CLI instead of an API key. The CLI remains a remote model
transport; it is not local inference.

The adapter uses the existing `StructuredOutputModel` boundary. It runs one
ephemeral turn in an isolated temporary directory, supplies the locked payload
through standard input, and constrains the final message with the evaluation's
JSON Schema.

## Run

Check that the CLI is installed and authenticated:

```bash
codex --version
codex login status
```

Run the diagnostic:

```bash
uv run run-relation-exclusion-evaluation run \
  docs/evidence/relation-exclusion-codex-cli \
  --provider codex-cli \
  --model gpt-5.6-sol \
  --reasoning-effort medium
```

New runs use one prompt: `proof-certificate-v1`, an original
relation-extraction adaptation of the evidence-before-conclusion,
counterexample, and alternative-hypothesis methods evaluated in
[Agentic Code Reasoning](https://arxiv.org/abs/2603.01896). Its output schema
uses the lean strict schema without descriptions. The receipt binds both
identifiers and their exact digests.

Older prompt and schema definitions remain internal only so historical
receipts can still be revalidated. They are not selectable for new runs.

The OpenAI Responses API remains the default:

```bash
uv run run-relation-exclusion-evaluation run \
  docs/evidence/relation-exclusion-openai \
  --provider openai
```

## Safety and receipts

The Codex adapter:

- ignores user configuration and execution rules;
- disables shell, MCP-adjacent, skill-search, plugin, app, browser, computer,
  memory, goal, image, hook, and multi-agent features;
- removes `OPENAI_API_KEY` and `CODEX_API_KEY` from the child environment;
- uses an ephemeral session and a read-only sandbox;
- rejects command, file, MCP, web, or other tool events;
- validates the final JSON locally;
- records the CLI version, thread identifier, token usage, disabled features,
  command digest, event-stream digest, and event types.

The receipt marks the requested output-token limit as unenforced because
`codex exec` exposes no equivalent to the Responses API's
`max_output_tokens`. A Codex CLI result is therefore a separate provider arm,
not an API-equivalent repetition.

## Measured diagnostic

Six Codex runs and the existing direct-API run use `gpt-5.6-sol` at medium
reasoning on the same diagnostic-v1 cases:

| Arm | Exact F1 | Polarity/modality F1 | Outcome accuracy | Direct denials | Input | Output | Seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Direct API baseline | 0.600 | 0.700 | 0.750 | 2/4 | 4,033 | 7,303 | 69 |
| Codex baseline R1 | 0.273 | 0.636 | 0.500 | 1/4 | 10,386 | 7,902 | 171 |
| Codex baseline R2 | 0.600 | 0.600 | 0.750 | 3/4 | 10,386 | 7,752 | 172 |
| Codex evidence-first R1 | 0.700 | 0.700 | 0.833 | 3/4 | 10,548 | 7,287 | 178 |
| Codex semi-formal R1 | 0.700 | 0.800 | 0.833 | 3/4 | 10,506 | 6,109 | 129 |
| Codex proof-certificate R1 | 0.700 | 0.900 | 0.833 | 3/4 | 10,589 | 6,272 | 142 |
| Codex proof-certificate + described schema R1 | 0.700 | 0.800 | 0.833 | 3/4 | 10,871 | 6,831 | 156 |

Every run was exactly grounded and emitted zero false control findings. All
five new Codex receipts pass integrity validation, contain no secret matches,
and fail the strict diagnostic-v1 quality gate.

The baseline swing from 0.273 to 0.600 exact F1 proves that one run cannot rank
the two transports. The treatment results supported adapting
`proof-certificate-v1` to the orthogonal v2 assertion-and-change-event
contract: it tied the best exact and outcome scores and produced the best
polarity/modality score. The lean `semi-formal-v1` arm remains historical
diagnostic evidence.

The described-schema arm did not improve exact F1 or outcome accuracy, reduced
polarity/modality F1 from 0.900 to 0.800, added 282 input tokens and 559 output
tokens, and added 14 seconds. One run cannot establish a general schema effect,
but it supplies no evidence for adopting descriptions. New runs therefore use
the lean schema. `described-v1` remains internal only so its historical receipt
can be revalidated.

Do not optimize further against diagnostic v1. Its exact-span oracle accepts
only one of multiple plausible FCC spans, treats conditional rule scope as a
flat modality, and expects proposed removals or suspensions to be denied
relations. The current ontology instead treats proposals as change events.
Use a new blinded, independently resolved v2 holdout for a transport or prompt
decision. The exposed pilot remains regression material.

## Focused v2 integration

Two direct-API diagnostics exercised only the five previously disputed cases.
They are exposed development evidence, not a provider comparison:

| V2 prompt | Semantic F1 | Recall | Exact grounding | Required preferred evidence |
| --- | ---: | ---: | ---: | ---: |
| Initial orthogonal instructions | 0.800 | 0.800 | 1.000 | 2/5 |
| Proof-certificate v2 | 0.909 | 1.000 | 1.000 | 5/5 |

The proof-certificate run recovered every required assertion and change event.
Its reported false positive was an allowed FCC attributed proposition whose
claimant was `the FCC` rather than provisional-oracle text `FCC`. The current
scorer normalizes that superficial determiner, matches allowed semantic
variants independently from evidence grade, and reports punctuation-only
evidence-boundary equivalence separately.

The v2 source contract now uses `attributed_source` rather than
`attributed_actor`, because a claimant can be a person, organization,
instrument, opinion, amendment, or other source. The provisional CFR terrain
annotation also records its governing `where` clause as explicit
conditionality.

These changes make the five cases a durable regression set. They do not make
the pilot an untouched holdout. Provider ranking still requires a new frozen
corpus, two blind human reviews, resolution, and repeated identical runs.

## Limits

Codex adds its own runtime context even when optional capabilities are
disabled. The bare profile reduces that overhead but cannot remove it. Compare
the direct API and Codex CLI as separate provider arms, with repeated runs,
rather than treating either as an equivalent repetition of the other.
