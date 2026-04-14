export type Docket = {
  docketId: string;
  agencyCode: string;
  title: string;
  docketType: string;
  modifyDate: string;
  abstract: string;
};

export type SearchSort = "relevance" | "recency";

export type SearchOptions = {
  limit?: number;
  offset?: number;
  sort?: SearchSort;
  agency?: string;
};

export type SearchResult = {
  docket: Docket;
  score: number;
  matchedTerms: string[];
};

export type SuggestResult = {
  suggestion: string;
  score: number;
};
