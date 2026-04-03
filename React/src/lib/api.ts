// API client for the exam archive backend
const API_BASE_URL = import.meta.env.VITE_API_URL || '';

  export interface SearchResult {
    doc_id: number;
    department: string;
    year: number;
    filename: string;
    /** first matched page (1-based) */
    page: number;
    /** total pages in the PDF */
    pages?: number;
    snippet: string;
    path?: string;
    author?: string;
  }

  export interface Document {
    id: number;
    department: string;
    year: number;
    filename: string;
    /** present when returned by /api/browse */
    page?: number;   // will be 1
    /** total pages in the PDF */
    pages?: number;
    path?: string;
    author?: string;
  }

  export interface Department {
    department: string;
    count: number;
  }

  export interface Year {
    year: number;
    count: number;
  }

  export interface Stats {
    documents: number;
    pages: number;
    page_fts_rows: number;
  }

  export interface Author {
    author: string;
    count: number;
  }

  export interface PipelineStatus {
    running: boolean;
    step: string;
    last_run: string | null;
    last_result: Record<string, unknown> | null;
    error: string | null;
  }

  export interface SearchParams {
    q: string;
    dept?: string;
    author?: string;     // NEW
    year_min?: number;
    year_max?: number;
    any?: boolean;
    limit?: number;
    offset?: number;
    group_by?: 'page' | 'doc';
  }

  export interface BrowseParams {
    dept?: string;
    author?: string;     // NEW
    year_min?: number;
    year_max?: number;
    limit?: number;
    offset?: number;
  }

class ApiClient {
  private baseUrl: string;
  private adminToken: string | null = null;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
    this.adminToken = sessionStorage.getItem('admin_token');
  }

  setAdminToken(token: string | null) {
    this.adminToken = token;
    if (token) {
      sessionStorage.setItem('admin_token', token);
    } else {
      sessionStorage.removeItem('admin_token');
    }
  }

  getAdminToken(): string | null {
    return this.adminToken;
  }

  private adminHeaders(): HeadersInit {
    return this.adminToken
      ? { 'Authorization': `Bearer ${this.adminToken}` }
      : {};
  }

  async search(params: SearchParams): Promise<SearchResult[]> {
    const queryParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        queryParams.append(key, String(value));
      }
    });

    const response = await fetch(`${this.baseUrl}/api/search?${queryParams}`);
    if (!response.ok) throw new Error('Search failed');
    return response.json();
  }

  async browse(params: BrowseParams = {}): Promise<Document[]> {
    const queryParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        queryParams.append(key, String(value));
      }
    });

    const response = await fetch(`${this.baseUrl}/api/browse?${queryParams}`);
    if (!response.ok) throw new Error('Browse failed');
    return response.json();
  }

  async getDepartments(): Promise<Department[]> {
    const response = await fetch(`${this.baseUrl}/api/departments`);
    if (!response.ok) throw new Error('Failed to fetch departments');
    return response.json();
  }

  async getYears(dept?: string): Promise<Year[]> {
    const params = dept ? `?dept=${encodeURIComponent(dept)}` : '';
    const response = await fetch(`${this.baseUrl}/api/years${params}`);
    if (!response.ok) throw new Error('Failed to fetch years');
    return response.json();
  }


  async getStats(): Promise<Stats> {
    const response = await fetch(`${this.baseUrl}/api/stats`);
    if (!response.ok) throw new Error('Failed to fetch stats');
    return response.json();
  }

  async getAuthors(): Promise<Author[]> {         // NEW
    const response = await fetch(`${this.baseUrl}/api/authors`);
    if (!response.ok) throw new Error('Failed to fetch authors');
    return response.json();
  }

  getViewUrl(docId: number): string {
    return `${this.baseUrl}/api/view/${docId}`;
  }

  getDownloadUrl(docId: number): string {
    return `${this.baseUrl}/api/download?doc_id=${docId}`;
  }

  async bulkDownload(docIds: number[]): Promise<Blob> {
    const response = await fetch(`${this.baseUrl}/api/download/bulk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ doc_ids: docIds }),
    });
    if (!response.ok) throw new Error('Bulk download failed');
    return response.blob();
  }

  // --- Admin / Pipeline ---

  async adminLogin(password: string): Promise<{ ok: boolean; token?: string }> {
    const response = await fetch(`${this.baseUrl}/api/admin/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password }),
    });
    const data = await response.json();
    if (data.ok && data.token) {
      this.setAdminToken(data.token);
    }
    return data;
  }

  adminLogout() {
    this.setAdminToken(null);
  }

  async getPipelineStatus(): Promise<PipelineStatus> {
    const response = await fetch(`${this.baseUrl}/api/admin/pipeline/status`, {
      headers: this.adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to fetch pipeline status');
    return response.json();
  }

  async runPipeline(): Promise<{ status: string }> {
    const response = await fetch(`${this.baseUrl}/api/admin/pipeline`, {
      method: 'POST',
      headers: this.adminHeaders(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(body.error || 'Failed to start pipeline');
    }
    return response.json();
  }

  async runDownload(): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/admin/download`, {
      method: 'POST',
      headers: this.adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to run download');
    return response.json();
  }

  async runIndex(): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/admin/index`, {
      method: 'POST',
      headers: this.adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to run indexing');
    return response.json();
  }

  async runOcr(): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/admin/ocr`, {
      method: 'POST',
      headers: this.adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to run OCR');
    return response.json();
  }

  async runFtsRebuild(): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/admin/fts-rebuild`, {
      method: 'POST',
      headers: this.adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to rebuild FTS');
    return response.json();
  }

  async exportResults(params: SearchParams): Promise<Blob> {
    const queryParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        queryParams.append(key, String(value));
      }
    });

    const response = await fetch(`${this.baseUrl}/api/export?${queryParams}`);
    if (!response.ok) throw new Error('Export failed');
    return response.blob();
  }
}

export const api = new ApiClient(API_BASE_URL);
