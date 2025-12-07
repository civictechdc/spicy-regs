const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000/api';

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

export async function getAgencies(): Promise<string[]> {
  const response = await fetch(`${API_BASE_URL}/agencies`);
  if (!response.ok) {
    throw new Error('Failed to fetch agencies');
  }
  return response.json();
}

export async function getDockets(agencyCode: string): Promise<string[]> {
  const response = await fetch(`${API_BASE_URL}/dockets?agency_code=${encodeURIComponent(agencyCode)}`);
  if (!response.ok) {
    throw new Error('Failed to fetch dockets');
  }
  return response.json();
}

export async function getRegulationData(
  agencyCode: string,
  dataType: DataType,
  docketId?: string
): Promise<RegulationData[]> {
  const params = new URLSearchParams();
  if (docketId) {
    params.append('docket_id', docketId);
  }
  const url = `${API_BASE_URL}/${agencyCode}/${dataType}${params.toString() ? `?${params.toString()}` : ''}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error('Failed to fetch regulation data');
  }
  return response.json();
}

