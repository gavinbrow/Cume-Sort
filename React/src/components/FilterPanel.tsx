import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Filter, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card } from "@/components/ui/card";
import { api } from "@/lib/api";

export interface Filters {
  dept?: string;
  year_min?: number;
  year_max?: number;
}

interface FilterPanelProps {
  filters: Filters;
  onChange: (filters: Filters) => void;
}

export const FilterPanel = ({ filters, onChange }: FilterPanelProps) => {
  const [isOpen, setIsOpen] = useState(false);
  
  const { data: departments } = useQuery({
    queryKey: ['departments'],
    queryFn: () => api.getDepartments(),
  });

  const { data: years } = useQuery({
    queryKey: ['years'],
    queryFn: () => api.getYears(),
  });

  const hasActiveFilters = filters.dept || filters.year_min || filters.year_max;

  const clearFilters = () => {
    onChange({});
  };

  return (
    <div className="w-full">
      <Button
        variant="outline"
        onClick={() => setIsOpen(!isOpen)}
        className="gap-2"
      >
        <Filter className="h-4 w-4" />
        Filters
        {hasActiveFilters && (
          <span className="bg-primary text-primary-foreground rounded-full px-2 py-0.5 text-xs">
            Active
          </span>
        )}
      </Button>

      {isOpen && (
        <Card className="mt-4 p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold">Filter Results</h3>
            {hasActiveFilters && (
              <Button variant="ghost" size="sm" onClick={clearFilters}>
                <X className="h-4 w-4 mr-2" />
                Clear All
              </Button>
            )}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label>Department</Label>
              <Select
                value={filters.dept || "all"}
                onValueChange={(value) => onChange({ ...filters, dept: value === "all" ? undefined : value })}
              >
                <SelectTrigger>
                  <SelectValue placeholder="All departments" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Departments</SelectItem>
                  {departments?.map((dept) => (
                    <SelectItem key={dept.department} value={dept.department}>
                      {dept.department} ({dept.count})
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label>Year From</Label>
              <Input
                type="number"
                placeholder="e.g., 2020"
                value={filters.year_min || ''}
                onChange={(e) => onChange({ 
                  ...filters, 
                  year_min: e.target.value ? parseInt(e.target.value) : undefined 
                })}
                min={years?.[0]?.year}
                max={years?.[years.length - 1]?.year}
              />
            </div>

            <div className="space-y-2">
              <Label>Year To</Label>
              <Input
                type="number"
                placeholder="e.g., 2024"
                value={filters.year_max || ''}
                onChange={(e) => onChange({ 
                  ...filters, 
                  year_max: e.target.value ? parseInt(e.target.value) : undefined 
                })}
                min={years?.[0]?.year}
                max={years?.[years.length - 1]?.year}
              />
            </div>
          </div>
        </Card>
      )}
    </div>
  );
};
