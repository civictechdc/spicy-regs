export type DataType = 'dockets' | 'documents' | 'comments';

export interface RegulationData {
  agency_code: string;
  docket_id: string;
  year: string;
  raw_json: string;
  docket_type?: string | null;
  modify_date?: string | null;
  title?: string | null;
  cached_at?: string;
  // Comment-specific fields
  comment_id?: string | null;
  comment?: string | null;
  document_type?: string | null;
  subtype?: string | null;
  posted_date?: string | null;
  receive_date?: string | null;
  organization?: string | null;
  [key: string]: any; // Allow for additional fields that may vary by data type
}
