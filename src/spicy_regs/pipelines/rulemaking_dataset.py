"""Materialized dataset pipeline for the rulemaking join surface."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, ClassVar

from cyclopts import App, Parameter

from spicy_regs.ontology.common import RunContext
from spicy_regs.pipelines.materialized import DatasetStage, MaterializedDatasetPipeline
from spicy_regs.transforms import (
    build_comment_periods,
    build_proceedings,
    build_regulatory_agenda,
    build_rule_targets,
)


class RulemakingDatasetPipeline(MaterializedDatasetPipeline):
    """Build one coherent generation of the rule-identity tables.

    A rule is a RIN on reginfo, a docket on regulations.gov, a set of CFR parts
    in the Code, and a document number in the Federal Register. Nothing joins
    the four. These stages do: ``rule_targets`` is the docket ↔ CFR ↔ RIN
    spine; ``proceedings`` promotes each rulemaking to a first-class record with
    its actions; ``regulatory_agenda`` links agenda items to those actions; and
    ``comment_periods`` materializes every comment interval, reopenings included.

    Stages run in dependency order and publish atomically as one generation
    under ``materialized/rulemaking/``, so a consumer never sees a spine from
    one run beside proceedings from another.
    """

    name: ClassVar[str] = "rulemaking-dataset"
    dataset_name: ClassVar[str] = "rulemaking"
    source_inputs: ClassVar[tuple[str, ...]] = (
        "dockets.parquet",
        "documents.parquet",
        "federal_register.parquet",
        "unified_agenda.parquet",
        "fr_docket_links.parquet",
    )
    prior_outputs: ClassVar[tuple[tuple[str, str], ...]] = (("proceedings.parquet", "_proceedings_prior.parquet"),)
    published_outputs: ClassVar[tuple[str, ...]] = (
        "rule_targets.parquet",
        "proceedings.parquet",
        "regulatory_agenda_items.parquet",
        "agenda_item_proceedings.parquet",
        "comment_periods.parquet",
    )

    def source_column_requirements(self) -> dict[str, tuple[str, ...]]:
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
                "modify_date",
                "comment_start_date",
                "comment_end_date",
            ),
            "federal_register.parquet": (
                "document_number",
                "title",
                "abstract",
                "document_type",
                "publication_date",
                "comments_close_on",
                "docket_ids_json",
                "regulation_id_numbers_json",
                "cfr_references_json",
            ),
            "unified_agenda.parquet": (
                "rin",
                "agenda_edition",
                "legal_authority_json",
                "cfr_references_json",
                "title",
                "agency_code",
                "rule_stage",
                "priority_category",
                "first_action_date",
                "next_action_date",
                "url",
            ),
            "fr_docket_links.parquet": (
                "document_number",
                "docket_id",
            ),
        }

    def stages(self) -> tuple[DatasetStage, ...]:
        def rule_targets(output_dir: Path, context: RunContext) -> None:
            build_rule_targets(output_dir, run_id=context.run_id, asserted_at=context.asserted_at)

        def proceedings(output_dir: Path, context: RunContext) -> None:
            build_proceedings(output_dir, run_id=context.run_id, asserted_at=context.asserted_at)

        def regulatory_agenda(output_dir: Path, context: RunContext) -> None:
            build_regulatory_agenda(output_dir, run_id=context.run_id, asserted_at=context.asserted_at)

        def comment_periods(output_dir: Path, context: RunContext) -> None:
            build_comment_periods(output_dir, run_id=context.run_id, asserted_at=context.asserted_at)

        return (
            DatasetStage(
                name="rule-targets",
                depends_on=(),
                outputs=("rule_targets.parquet",),
                build=rule_targets,
            ),
            DatasetStage(
                name="proceedings",
                depends_on=("rule-targets",),
                outputs=("proceedings.parquet",),
                build=proceedings,
            ),
            DatasetStage(
                name="regulatory-agenda",
                depends_on=("proceedings",),
                outputs=("regulatory_agenda_items.parquet", "agenda_item_proceedings.parquet"),
                build=regulatory_agenda,
            ),
            DatasetStage(
                name="comment-periods",
                depends_on=("proceedings",),
                outputs=("comment_periods.parquet",),
                build=comment_periods,
            ),
        )


app = App(
    name="materialize-rulemaking",
    help="Build and atomically publish the rulemaking join-surface dataset.",
)


@app.default
def main(
    *,
    output_dir: Annotated[Path | None, Parameter(help="Output directory")] = None,
    skip_upload: Annotated[bool, Parameter(help="Skip R2 upload (recommended while vetting)")] = True,
    allow_bootstrap: Annotated[
        bool,
        Parameter(help="Allow a first publication with no prior rulemaking generation"),
    ] = False,
) -> None:
    RulemakingDatasetPipeline(
        output_dir=output_dir,
        skip_upload=skip_upload,
        allow_bootstrap=allow_bootstrap,
    ).run()


if __name__ == "__main__":
    app()
