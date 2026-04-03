import { FileText, Download, Eye } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { api } from "@/lib/api";
import type { Document, SearchResult } from "@/lib/api";

interface DocumentCardProps {
  document: Document | SearchResult;
  selected?: boolean;
  onToggleSelect?: (id: number) => void;
  showSnippet?: boolean;
}

export const DocumentCard = ({ document, selected, onToggleSelect, showSnippet }: DocumentCardProps) => {
  const isSearchResult = "doc_id" in document;
  const docId = isSearchResult ? (document as SearchResult).doc_id : (document as Document).id;

  // Data that may or may not exist depending on route
  const snippet: string | undefined =
    (isSearchResult && "snippet" in document ? (document as SearchResult).snippet : undefined) || undefined;

  // First matched page (1-based) if this came from /api/search with a text hit.
  // For browse (or search-without-keywords), backend often sets page=1; we hide that unless there is a snippet (i.e., a real text hit).
  const page: number | undefined =
    (isSearchResult && "page" in document ? (document as SearchResult).page : (document as Document).page) as
      | number
      | undefined;

  // Total pages in the PDF (backend now sends this for both /api/search and /api/browse)
  const pages: number | undefined =
    (("pages" in document ? (document as any).pages : undefined) as number | undefined) ?? undefined;

  const isTextHit = Boolean(snippet && snippet.length > 0);

  const handleView = () => {
    window.open(api.getViewUrl(docId), "_blank");
  };

  const handleDownload = () => {
    window.open(api.getDownloadUrl(docId), "_blank");
  };

  return (
    <Card className="p-4 hover:shadow-card-hover transition-all duration-200">
      <div className="flex gap-4">
        {onToggleSelect && (
          <div className="flex-shrink-0 pt-1">
            <Checkbox checked={selected} onCheckedChange={() => onToggleSelect(docId)} />
          </div>
        )}

        <div className="flex-1 min-w-0">
          <div className="flex items-start gap-3">
            <div className="flex-shrink-0 p-2 bg-accent rounded-lg">
              <FileText className="h-6 w-6 text-accent-foreground" />
            </div>

            <div className="flex-1 min-w-0">
              <h3 className="font-semibold text-card-foreground truncate">{document.filename}</h3>

              <div className="flex flex-wrap gap-2 mt-1 text-sm text-muted-foreground">
                <span className="bg-secondary px-2 py-0.5 rounded">{document.department}</span>
                <span className="bg-secondary px-2 py-0.5 rounded">{document.year}</span>

                {/* Show "Match on page X" ONLY when we truly have a text match */}
                {isTextHit && typeof page === "number" && (
                  <span className="bg-accent px-2 py-0.5 rounded text-accent-foreground">
                    Match on page {page}
                  </span>
                )}

                {/* Always show total page count if we have it */}
                {typeof pages === "number" && pages > 0 && (
                  <span className="bg-secondary px-2 py-0.5 rounded">{pages} pages</span>
                )}
              </div>

              {showSnippet && isTextHit && (
                <p className="mt-2 text-sm text-muted-foreground line-clamp-2">{snippet}</p>
              )}
            </div>
          </div>
        </div>

        <div className="flex-shrink-0 flex gap-2">
          <Button variant="outline" size="icon" onClick={handleView} title="View PDF">
            <Eye className="h-4 w-4" />
          </Button>
          <Button variant="outline" size="icon" onClick={handleDownload} title="Download PDF">
            <Download className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </Card>
  );
};
