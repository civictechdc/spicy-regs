import { FIELD_MAPPINGS } from "./constants";
import { RegulationsDataTypes } from "./models";

export function buildBaseQuery(
  dataType: RegulationsDataTypes,
  agencyCode?: string,
  docketId?: string
): string {
  // Mapping keys are "dockets", "comments", "documents" (matching values of enum)
  const mapping = FIELD_MAPPINGS[dataType];
  const fields = mapping.fields.join(",\n        ");

  let path = "";
  if (agencyCode && docketId) {
    path = `s3://mirrulations/raw-data/${agencyCode}/${docketId}/text-${docketId}/${mapping.path_pattern}`;
  } else if (agencyCode) {
    path = `s3://mirrulations/raw-data/${agencyCode}/*/*/${mapping.path_pattern}`;
  } else {
    // Note: Wildcard matching might be slow or unsupported depending on file count/structure support in httpfs
    path = `s3://mirrulations/raw-data/*/*/${mapping.path_pattern}`;
  }

  // Double quotes for strings in SQL used in Python were single quotes, which is standard SQL.
  // We keep single quotes for SQL literals.
  
  return `
    SELECT
        f.agency_code,
        f.docket_id,
        f.year,
        f.content AS raw_json,
        ${fields}
    FROM (
        SELECT
            filename,
            content,
            split_part(filename, '/', 5) as agency_code,
            split_part(filename, '/', 6) as docket_id,
            split_part(split_part(filename, '/', 6), '-', 2) as year
        FROM read_text('${path}')
    ) f
    `;
}

export function buildWhereClause(agencyCode?: string, docketId?: string): string {
  const whereClause: string[] = [];
  if (agencyCode) {
    whereClause.push(`agency_code = '${agencyCode.toUpperCase()}'`);
  }
  if (docketId) {
    whereClause.push(`docket_id = '${docketId.toUpperCase()}'`);
  }
  
  if (whereClause.length === 0) {
    return "1=1";
  }
  return whereClause.join(" AND ");
}
