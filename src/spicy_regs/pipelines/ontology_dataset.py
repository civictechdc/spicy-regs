"""Materialized dataset pipeline for rule identity and retrieval ontology."""

from __future__ import annotations

from pathlib import Path
from shutil import copy2
from typing import Annotated, ClassVar

from cyclopts import App, Parameter

from spicy_regs.ontology.common import RunContext
from spicy_regs.pipelines.materialized import DatasetStage, MaterializedDatasetPipeline
from spicy_regs.transforms import (
    build_authority_edges,
    build_comment_periods,
    build_concept_assignments,
    build_concept_events,
    build_concepts,
    build_proceedings,
    build_rule_targets,
)


class OntologyDatasetPipeline(MaterializedDatasetPipeline):
    """Build one coherent generation of all rule-identity and ontology tables."""

    name: ClassVar[str] = "ontology-dataset"
    dataset_name: ClassVar[str] = "ontology"
    source_inputs: ClassVar[tuple[str, ...]] = (
        "dockets.parquet",
        "documents.parquet",
        "federal_register.parquet",
        "unified_agenda.parquet",
        "fr_docket_links.parquet",
    )
    prior_outputs: ClassVar[tuple[tuple[str, str], ...]] = (
        ("proceedings.parquet", "_proceedings_prior.parquet"),
        ("concepts.parquet", "_concepts_prior.parquet"),
        ("concept_assignments.parquet", "_concept_assignments_prior.parquet"),
        ("concept_events.parquet", "_concept_events_prior.parquet"),
    )
    published_outputs: ClassVar[tuple[str, ...]] = (
        "rule_targets.parquet",
        "authority_edges.parquet",
        "proceedings.parquet",
        "comment_periods.parquet",
        "concepts.parquet",
        "concept_assignments.parquet",
        "concept_events.parquet",
    )

    def __init__(self, *, full_refresh: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.full_refresh = full_refresh

    def source_column_requirements(self) -> dict[str, tuple[str, ...]]:
        federal_register = [
            "document_number",
            "title",
            "abstract",
            "document_type",
            "publication_date",
            "comments_close_on",
            "docket_ids_json",
            "regulation_id_numbers_json",
            "cfr_references_json",
        ]
        if self.full_refresh:
            federal_register.append("topics_json")
        return {
            "dockets.parquet": (
                "docket_id",
                "rin",
                "docket_type",
                "title",
                "abstract",
                "agency_code",
                "modify_date",
            ),
            "documents.parquet": (
                "document_id",
                "docket_id",
                "additional_rins",
                "fr_doc_num",
                "document_type",
                "title",
                "agency_code",
                "posted_date",
                "comment_start_date",
                "comment_end_date",
            ),
            "federal_register.parquet": tuple(federal_register),
            "unified_agenda.parquet": (
                "rin",
                "agenda_edition",
                "legal_authority_json",
                "cfr_references_json",
                "title",
                "agency_code",
                "rule_stage",
                "first_action_date",
            ),
            "fr_docket_links.parquet": (
                "document_number",
                "docket_id",
            ),
        }

    def prepare(
        self,
        output_dir: Path,
        *,
        previous_manifest: dict | None,
        context: RunContext,
    ) -> None:
        del previous_manifest, context
        if self.full_refresh:
            return
        for output_name, prior_name in (
            ("concepts.parquet", "_concepts_prior.parquet"),
            ("concept_assignments.parquet", "_concept_assignments_prior.parquet"),
            ("concept_events.parquet", "_concept_events_prior.parquet"),
        ):
            prior = output_dir / prior_name
            if not prior.exists():
                raise RuntimeError(
                    f"An identity-only ontology refresh requires a prior complete generation; missing {output_name}"
                )
            copy2(prior, output_dir / output_name)

    def validate_before_publish(self, manifest_path: Path) -> None:
        """Refuse ontology publication when any mapped carrier row is invalid."""
        # Imported lazily because the receipt validator binds this concrete
        # pipeline's declared output set.
        from spicy_regs.ontology.receipt import validate_generation

        result = validate_generation(manifest_path)
        if result["status"] != "pass":
            count = result["failures"]["total"]
            raise RuntimeError(
                f"Refusing to publish ontology generation: "
                f"corpus receipt found {count} validation failure(s)"
            )

    def stages(self) -> tuple[DatasetStage, ...]:
        def rule_targets(output_dir: Path, context: RunContext) -> None:
            build_rule_targets(
                output_dir,
                run_id=context.run_id,
                asserted_at=context.asserted_at,
            )

        def authority_edges(output_dir: Path, context: RunContext) -> None:
            build_authority_edges(
                output_dir,
                run_id=context.run_id,
                asserted_at=context.asserted_at,
            )

        def proceedings(output_dir: Path, context: RunContext) -> None:
            build_proceedings(
                output_dir,
                run_id=context.run_id,
                asserted_at=context.asserted_at,
            )

        def comment_periods(output_dir: Path, context: RunContext) -> None:
            build_comment_periods(
                output_dir,
                run_id=context.run_id,
                asserted_at=context.asserted_at,
            )

        stages = [
            DatasetStage(
                name="proceedings",
                depends_on=("rule-targets", "authority-edges"),
                outputs=("proceedings.parquet",),
                build=proceedings,
            ),
            DatasetStage(
                name="comment-periods",
                depends_on=("proceedings",),
                outputs=("comment_periods.parquet",),
                build=comment_periods,
            ),
            DatasetStage(
                name="rule-targets",
                depends_on=(),
                outputs=("rule_targets.parquet",),
                build=rule_targets,
            ),
            DatasetStage(
                name="authority-edges",
                depends_on=(),
                outputs=("authority_edges.parquet",),
                build=authority_edges,
            ),
        ]
        if self.full_refresh:

            def concepts(output_dir: Path, context: RunContext) -> None:
                build_concepts(
                    output_dir,
                    run_id=context.run_id,
                    asserted_at=context.asserted_at,
                )

            def assignments(output_dir: Path, context: RunContext) -> None:
                build_concept_assignments(
                    output_dir,
                    run_id=context.run_id,
                    asserted_at=context.asserted_at,
                )

            def events(output_dir: Path, context: RunContext) -> None:
                build_concept_events(
                    output_dir,
                    run_id=context.run_id,
                    asserted_at=context.asserted_at,
                )

            stages.extend(
                (
                    DatasetStage(
                        name="concepts",
                        depends_on=(),
                        outputs=(
                            "concepts.parquet",
                            "concept_events.parquet",
                            "concept_merge_review.jsonl",
                        ),
                        build=concepts,
                    ),
                    DatasetStage(
                        name="concept-assignments",
                        depends_on=("concepts",),
                        outputs=(
                            "concepts.parquet",
                            "concept_assignments.parquet",
                            "concept_events.parquet",
                        ),
                        build=assignments,
                    ),
                    DatasetStage(
                        name="concept-events",
                        depends_on=("concept-assignments",),
                        outputs=("concept_events.parquet",),
                        build=events,
                    ),
                )
            )
        return tuple(stages)


app = App(
    name="materialize-ontology",
    help="Build and atomically publish the rule-identity and ontology dataset.",
)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[
        bool,
        Parameter(help="Skip R2 upload (recommended while vetting)"),
    ] = True,
    full_refresh: Annotated[
        bool,
        Parameter(help="Refresh concept state as well as daily identity tables"),
    ] = True,
    allow_bootstrap: Annotated[
        bool,
        Parameter(help="Allow a first publication with no prior ontology generation"),
    ] = False,
) -> None:
    OntologyDatasetPipeline(
        output_dir=output_dir,
        skip_upload=skip_upload,
        full_refresh=full_refresh,
        allow_bootstrap=allow_bootstrap,
    ).run()


if __name__ == "__main__":
    app()
