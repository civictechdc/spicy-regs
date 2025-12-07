
export function stripQuotes(str: string | null | undefined): string {
  if (!str) return '';
  return str.replace(/^"|"$/g, '').trim();
}

export function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return '';
  try {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { 
      year: 'numeric', 
      month: 'short', 
      day: 'numeric' 
    });
  } catch {
    return dateStr;
  }
}

export function parseRawJson(rawJson: string | undefined): any {
  if (!rawJson) return null;
  try {
    return JSON.parse(rawJson);
  } catch {
    return null;
  }
}
