import { runQuery } from "./client";
import { FIELD_MAPPINGS } from "./constants";
import { RegulationsDataTypes } from "./models";

export async function createEmptyTables() {
  console.log("Creating empty tables...");
  for (const dataType of Object.values(RegulationsDataTypes)) {
    const tableName = `${dataType}_cache`;
    
    // Build column definitions
    const columns = [
      "agency_code VARCHAR",
      "docket_id VARCHAR",
      "year VARCHAR",
      "raw_json TEXT",
    ];

    if (dataType === RegulationsDataTypes.Dockets) {
      columns.push("docket_type VARCHAR", "modify_date TIMESTAMP", "title TEXT");
    } else if (dataType === RegulationsDataTypes.Comments) {
      columns.push(
        "comment_id VARCHAR",
        "category VARCHAR",
        "comment TEXT",
        "document_type VARCHAR",
        "modify_date TIMESTAMP",
        "posted_date TIMESTAMP",
        "receive_date TIMESTAMP",
        "subtype VARCHAR",
        "title TEXT",
        "withdrawn BOOLEAN"
      );
    } else if (dataType === RegulationsDataTypes.Documents) {
      columns.push(
        "document_id VARCHAR",
        "category VARCHAR",
        "document_type VARCHAR",
        "comment_start_date TIMESTAMP",
        "comment_end_date TIMESTAMP",
        "modify_date TIMESTAMP",
        "posted_date TIMESTAMP",
        "receive_date TIMESTAMP",
        "page_count INTEGER",
        "withdrawn BOOLEAN",
        "title TEXT"
      );
    }

    columns.push("cached_at TIMESTAMP");
    const columnDefs = columns.join(",\n      ");
    
    const query = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        ${columnDefs}
      )
    `;
    
    await runQuery(query);
  }
  console.log("Tables created successfully!");
}

export async function createIndexes() {
  console.log("Creating indexes...");
  for (const dataType of Object.values(RegulationsDataTypes)) {
    const tableName = `${dataType}_cache`;
    await runQuery(`CREATE INDEX IF NOT EXISTS idx_${dataType}_agency ON ${tableName}(agency_code)`);
    await runQuery(`CREATE INDEX IF NOT EXISTS idx_${dataType}_docket_id ON ${tableName}(docket_id)`);
  }
  console.log("Indexes created successfully!");
}

export async function initializeDatabase() {
  await createEmptyTables();
  await createIndexes();
  
  try {
    await runQuery(`ALTER TABLE documents_cache ADD COLUMN title TEXT`);
    console.log("Migrated documents_cache: added title column");
  } catch (e: any) {
    // Column likely already exists
  }
  
  console.log("Database initialized successfully!");
}
