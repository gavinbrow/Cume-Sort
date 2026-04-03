import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { BookOpen, Search as SearchIcon, FolderOpen, ChevronRight, Settings } from "lucide-react";
import { Link } from "react-router-dom";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { SearchBar } from "@/components/SearchBar";
import { FilterPanel, type Filters } from "@/components/FilterPanel";
import { DocumentCard } from "@/components/DocumentCard";
import { BulkActions } from "@/components/BulkActions";
import { api } from "@/lib/api";
import type { Author, Department, Document, SearchResult, Stats, Year } from "@/lib/api";

const YEAR_MIN_BOUND = 1989;
const YEAR_MAX_BOUND = 2030;

function clampYear(y: number) {
  return Math.min(YEAR_MAX_BOUND, Math.max(YEAR_MIN_BOUND, y));
}

function clampFilters(f: Filters): Filters {
  const withBounds: Filters = { ...f };
  withBounds.year_min = clampYear(withBounds.year_min ?? YEAR_MIN_BOUND);
  withBounds.year_max = clampYear(withBounds.year_max ?? YEAR_MAX_BOUND);
  if (withBounds.year_min > withBounds.year_max) {
    const t = withBounds.year_min;
    withBounds.year_min = withBounds.year_max;
    withBounds.year_max = t;
  }
  return withBounds;
}

const Index = () => {
  const [activeTab, setActiveTab] = useState<"search" | "browse">("search");

  // Search state
  const [searchQuery, setSearchQuery] = useState("");
  const [filters, setFilters] = useState<Filters>({});
  const [author, setAuthor] = useState<string>(""); // "" → no author filter
  const [searchTriggered, setSearchTriggered] = useState(false); // require click before fetching
  const [currentPage, setCurrentPage] = useState(0);
  const limit = 20;

  // Selection
  const [selectedIds, setSelectedIds] = useState<number[]>([]);

  // Browse drilldown
  const [selectedDept, setSelectedDept] = useState<string | null>(null);
  const [selectedYear, setSelectedYear] = useState<number | null>(null);

  // ----- Stats -----
  const { data: stats } = useQuery<Stats>({
    queryKey: ['stats'],
    queryFn: () => api.getStats(),
  });

  // ----- Facets -----
  const { data: departments } = useQuery<Department[]>({
    queryKey: ['departments'],
    queryFn: () => api.getDepartments(),
  });

  const { data: authors } = useQuery<Author[]>({
    queryKey: ['authors'],
    queryFn: () => api.getAuthors(),
  });

  // Author dropdown (global, above both tabs)
  const AuthorSelect = (
    <div className="mt-4">
      <label className="block text-sm font-medium mb-1">Author</label>
      <Select
        value={author || undefined}
        onValueChange={(v) => {
          setAuthor(v === "__ALL_AUTHORS__" ? "" : v);
          setCurrentPage(0);
          setSearchTriggered(false); // changing filters should not auto-hit backend
        }}
      >
        <SelectTrigger className="w-full md:w-80">
          <SelectValue placeholder="All authors" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="__ALL_AUTHORS__">All authors</SelectItem>
          {authors?.map((a) => (
            <SelectItem key={a.author} value={a.author}>
              {a.author}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );

  const boundedFilters = useMemo(() => clampFilters(filters), [filters]);

  // ----- SEARCH TAB: requires pressing Search -----
  const { data: searchData, isLoading: searchLoading } = useQuery<SearchResult[]>({
    queryKey: ['searchLike', searchQuery, boundedFilters, author, currentPage],
    queryFn: async (): Promise<SearchResult[]> => {
      if (searchQuery.trim()) {
        // keyword search
        return api.search({
          q: searchQuery,
          ...boundedFilters,
          author: author || undefined,
          limit,
          offset: currentPage * limit,
          group_by: 'doc',
        });
      } else {
        // filters-only "search": use browse under the hood and normalize
        const docs = await api.browse({
          ...boundedFilters,
          author: author || undefined,
          limit,
          offset: currentPage * limit,
        });
        return docs.map((d) => ({
          doc_id: d.id,
          department: d.department,
          year: d.year,
          filename: d.filename,
          page: (d as any).page ?? 1,     // backend sets 1 for browse
          pages: (d as any).pages,        // carry total pages to the card if present
          snippet: "",
          path: d.path,
          author: d.author,
        })) as unknown as SearchResult[];
      }
    },
    enabled: activeTab === "search" && searchTriggered, // only after pressing Search
    retry: 0,
    staleTime: 0,
    gcTime: 5 * 60 * 1000,
  });

  const searchResults: SearchResult[] = searchData ?? [];

  // NEW: sort newest → oldest for SEARCH tab
  const sortedSearchResults = useMemo(() => {
    const arr = [...searchResults];
    arr.sort((a, b) => {
      if (b.year !== a.year) return b.year - a.year; // year DESC
      if ((a.page ?? 0) !== (b.page ?? 0)) return (a.page ?? 0) - (b.page ?? 0); // tie: page ASC
      return String(a.filename).localeCompare(String(b.filename)); // tie: name ASC
    });
    return arr;
  }, [searchResults]);

  // When filters / query change, require pressing Search again
  const handleFiltersChange = (f: Filters) => {
    setFilters(f);
    setCurrentPage(0);
    setSearchTriggered(false);
  };

  // ----- BROWSE TAB: dept → years → docs -----
  const { data: yearsForDept } = useQuery<Year[]>({
    queryKey: ['yearsByDept', selectedDept],
    queryFn: () => api.getYears(selectedDept || undefined),
    enabled: activeTab === "browse" && !!selectedDept,
  });

  const filteredYearsForDept = useMemo(
    () =>
      (yearsForDept ?? [])
        .filter(y => y.year >= YEAR_MIN_BOUND && y.year <= YEAR_MAX_BOUND)
        .sort((a, b) => b.year - a.year), // ensure newest → oldest
    [yearsForDept]
  );

  const { data: docsForDeptYear, isLoading: docsForDeptYearLoading } = useQuery<Document[]>({
    queryKey: ['docsByDeptYear', selectedDept, selectedYear, author, currentPage],
    queryFn: () => api.browse({
      dept: selectedDept || undefined,
      year_min: selectedYear != null ? clampYear(selectedYear) : YEAR_MIN_BOUND,
      year_max: selectedYear != null ? clampYear(selectedYear) : YEAR_MAX_BOUND,
      author: author || undefined,
      limit,
      offset: currentPage * limit,
    }),
    enabled: activeTab === "browse" && !!selectedDept && selectedYear != null,
    retry: 0,
  });

  // NEW: sort newest → oldest for BROWSE doc list
  const sortedDocsForDeptYear = useMemo(() => {
    const arr = [...(docsForDeptYear ?? [])];
    arr.sort((a, b) => (b.year - a.year) || String(a.filename).localeCompare(String(b.filename)));
    return arr;
  }, [docsForDeptYear]);

  const toggleSelect = (id: number) => {
    setSelectedIds(prev => prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]);
  };
  const clearSelection = () => setSelectedIds([]);

  return (
    <div className="min-h-screen bg-background">
      {/* Hero */}
      <header className="bg-hero-gradient text-white py-16 px-4">
        <div className="container mx-auto max-w-6xl">
          <div className="flex items-center gap-3 mb-4">
            <BookOpen className="h-10 w-10" />
            <h1 className="text-4xl md:text-5xl font-bold">UARK Chem Exam Archive</h1>
          </div>
          <div className="flex items-center justify-between mb-8">
            <p className="text-xl text-white/90">
              Search and browse through thousands of past exams. By: Gavin Brown
            </p>
            <Link
              to="/admin"
              className="inline-flex items-center gap-1.5 text-white/70 hover:text-white text-sm transition-colors"
              title="Admin Panel"
            >
              <Settings className="h-5 w-5" />
              <span className="hidden sm:inline">Admin</span>
            </Link>
          </div>

          {stats && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 max-w-2xl">
              <Card className="p-4 bg-white/10 backdrop-blur border-white/20">
                <div className="text-3xl font-bold">{stats.documents.toLocaleString()}</div>
                <div className="text-sm text-white/80">Documents</div>
              </Card>
              <Card className="p-4 bg-white/10 backdrop-blur border-white/20">
                <div className="text-3xl font-bold">{stats.pages.toLocaleString()}</div>
                <div className="text-sm text-white/80">Pages</div>
              </Card>
              <Card className="p-4 bg-white/10 backdrop-blur border-white/20">
                <div className="text-3xl font-bold">{stats.page_fts_rows.toLocaleString()}</div>
                <div className="text-sm text-white/80">Searchable Pages</div>
              </Card>
            </div>
          )}
        </div>
      </header>

      {/* Filters always visible */}
      <section className="container mx-auto max-w-6xl px-4 -mt-10">
        <Card className="p-6">
          <div className="grid gap-6 md:grid-cols-2">
            <div>
              <h2 className="text-xl font-semibold mb-2">Filters</h2>
              <FilterPanel
                filters={filters}
                onChange={handleFiltersChange}
              />
              {AuthorSelect}
            </div>
            <div>
              <h2 className="text-xl font-semibold mb-2">Search (optional)</h2>
              <SearchBar
                value={searchQuery}
                onChange={(v) => { setSearchQuery(v); setSearchTriggered(false); }}
                onSearch={() => { setCurrentPage(0); setSearchTriggered(true); }} // single source of truth
                placeholder="Search exam content (optional)…"
              />
            </div>
          </div>
        </Card>
      </section>

      {/* Main */}
      <main className="container mx-auto max-w-6xl px-4 py-8">
        <Tabs
          value={activeTab}
          onValueChange={(v) => {
            setActiveTab(v as "search" | "browse");
            setCurrentPage(0);
          }}
          className="w-full"
        >
          <TabsList className="grid w-full max-w-md mx-auto grid-cols-2 mb-8">
            <TabsTrigger value="search" className="gap-2">
              <SearchIcon className="h-4 w-4" />
              Search
            </TabsTrigger>
            <TabsTrigger value="browse" className="gap-2">
              <FolderOpen className="h-4 w-4" />
              Browse
            </TabsTrigger>
          </TabsList>

          {/* SEARCH TAB */}
          <TabsContent value="search" className="space-y-6">
            {!searchTriggered && (
              <Card className="p-12 text-center">
                <SearchIcon className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                <h3 className="text-xl font-semibold mb-2">Ready to search</h3>
                <p className="text-muted-foreground">
                  Adjust filters and press <span className="font-semibold">Search</span>.
                </p>
              </Card>
            )}

            {searchTriggered && searchLoading && (
              <Card className="p-12 text-center"><div className="animate-pulse">Loading…</div></Card>
            )}

            {searchTriggered && !searchLoading && sortedSearchResults.length > 0 && (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h2 className="text-2xl font-semibold">
                    Results
                    <span className="ml-2 text-muted-foreground text-base">
                      ({sortedSearchResults.length} shown)
                    </span>
                  </h2>
                </div>

                <div className="space-y-3">
                  {sortedSearchResults.map((result) => (
                    <DocumentCard
                      key={`${result.doc_id}-${result.page ?? 1}`}
                      document={result}
                      selected={selectedIds.includes(result.doc_id)}
                      onToggleSelect={toggleSelect}
                      showSnippet={Boolean((result as any).snippet)}
                    />
                  ))}
                </div>

                <div className="flex justify-center gap-2">
                  <Button
                    variant="outline"
                    onClick={() => setCurrentPage(p => Math.max(0, p - 1))}
                    disabled={currentPage === 0}
                  >
                    Previous
                  </Button>
                  <Button
                    variant="outline"
                    onClick={() => setCurrentPage(p => p + 1)}
                    disabled={sortedSearchResults.length < limit}
                  >
                    Next
                  </Button>
                </div>
              </div>
            )}

            {searchTriggered && !searchLoading && sortedSearchResults.length === 0 && (
              <Card className="p-12 text-center">
                <h3 className="text-xl font-semibold mb-2">No results</h3>
                <p className="text-muted-foreground">Try adjusting your filters or add a keyword.</p>
              </Card>
            )}
          </TabsContent>

          {/* BROWSE TAB */}
          <TabsContent value="browse" className="space-y-6">
            <Card className="p-6">
              <h2 className="text-xl font-semibold mb-4">Departments</h2>
              <div className="grid gap-2 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
                {(departments ?? []).map((d) => (
                  <Button
                    key={d.department}
                    variant={selectedDept === d.department ? "default" : "outline"}
                    onClick={() => {
                      const same = selectedDept === d.department;
                      setSelectedDept(same ? null : d.department);
                      setSelectedYear(null);
                      setCurrentPage(0);
                    }}
                    className="justify-between"
                  >
                    <span>{d.department}</span>
                    <span className="text-xs opacity-75 flex items-center gap-1">
                      {d.count?.toLocaleString?.() ?? d.count}
                      <ChevronRight className="h-3 w-3" />
                    </span>
                  </Button>
                ))}
              </div>
            </Card>

            {selectedDept && (
              <Card className="p-6">
                <h3 className="text-lg font-semibold mb-4">Years in {selectedDept}</h3>
                <div className="flex flex-wrap gap-2">
                  {filteredYearsForDept.map((y) => (
                    <Button
                      key={y.year}
                      variant={selectedYear === y.year ? "default" : "outline"}
                      onClick={() => { setSelectedYear(y.year); setCurrentPage(0); }}
                    >
                      {y.year} <span className="ml-2 text-xs opacity-70">({y.count})</span>
                    </Button>
                  ))}
                </div>
                {filteredYearsForDept.length === 0 && (
                  <p className="text-muted-foreground mt-3">
                    No years between {YEAR_MIN_BOUND}–{YEAR_MAX_BOUND}.
                  </p>
                )}
              </Card>
            )}

            {selectedDept && selectedYear != null && (
              <>
                {docsForDeptYearLoading && (
                  <Card className="p-12 text-center"><div className="animate-pulse">Loading…</div></Card>
                )}

                {sortedDocsForDeptYear && sortedDocsForDeptYear.length > 0 && (
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <h2 className="text-2xl font-semibold">
                        {selectedDept} — {selectedYear}
                        <span className="ml-2 text-muted-foreground text-base">
                          ({sortedDocsForDeptYear.length} shown)
                        </span>
                      </h2>
                    </div>

                    <div className="space-y-3">
                      {sortedDocsForDeptYear.map((doc) => (
                        <DocumentCard
                          key={doc.id}
                          document={doc}
                          selected={selectedIds.includes(doc.id)}
                          onToggleSelect={toggleSelect}
                        />
                      ))}
                    </div>

                    <div className="flex justify-center gap-2">
                      <Button
                        variant="outline"
                        onClick={() => setCurrentPage(p => Math.max(0, p - 1))}
                        disabled={currentPage === 0}
                      >
                        Previous
                      </Button>
                      <Button
                        variant="outline"
                        onClick={() => setCurrentPage(p => p + 1)}
                        disabled={(sortedDocsForDeptYear ?? []).length < limit}
                      >
                        Next
                      </Button>
                    </div>
                  </div>
                )}

                {sortedDocsForDeptYear && sortedDocsForDeptYear.length === 0 && !docsForDeptYearLoading && (
                  <Card className="p-12 text-center">
                    <h3 className="text-xl font-semibold mb-2">No documents found</h3>
                    <p className="text-muted-foreground">Try another year or adjust the Author filter.</p>
                  </Card>
                )}
              </>
            )}
          </TabsContent>
        </Tabs>
      </main>

      <BulkActions selectedIds={selectedIds} onClear={clearSelection} />
    </div>
  );
};

export default Index;
