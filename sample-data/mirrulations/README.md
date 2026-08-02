# Sample Data from Mirrulations

In this directory, you can find sample json files retrieved from the mirrulations S3 bucket.

The data here is retrieved from `s3://mirrulations/raw-data/ACF/ACF-2025-0038`.

There are generally 3 types of datasets

1. Docket

    This json file has high level overview of the docket

2. Documents

    This json holds information about any attachments that are relevant to the docket.

3. Comment

    This json holds information about comments on the docket including any attachments to any particular comment.

`document-release-file-manifest-v1.json` binds the checked-in document JSON and
PDF to their exact SHA-256 digests, upstream URLs, and capture evidence. Run the
actual-file release path with:

```console
uv run --frozen build-document-release-from-files \
  --manifest sample-data/mirrulations/document-release-file-manifest-v1.json \
  --output-dir output/mirrulations-document-release
```

The output directory contains `document-release.json` plus content-addressed
copies of every source rendition. A consumer can therefore re-hash the bytes
named by each `source_native_path` without importing this repository. Full
release validation also requires the Rulespec Core file whose ID and digest
are pinned in `document-release.json`; the default repository Core is a
conformance fixture.
