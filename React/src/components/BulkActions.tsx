import { Download, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { api } from "@/lib/api";
import { toast } from "@/hooks/use-toast";

interface BulkActionsProps {
  selectedIds: number[];
  onClear: () => void;
}

export const BulkActions = ({ selectedIds, onClear }: BulkActionsProps) => {
  if (selectedIds.length === 0) return null;

  const handleBulkDownload = async () => {
    try {
      toast({
        title: "Preparing download...",
        description: `Creating ZIP file with ${selectedIds.length} documents`,
      });

      const blob = await api.bulkDownload(selectedIds);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'exam_selection.zip';
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      toast({
        title: "Download complete!",
        description: `Successfully downloaded ${selectedIds.length} documents`,
      });
    } catch (error) {
      toast({
        title: "Download failed",
        description: "Unable to download selected documents",
        variant: "destructive",
      });
    }
  };

  return (
    <Card className="fixed bottom-6 left-1/2 -translate-x-1/2 p-4 shadow-lg z-50 bg-card">
      <div className="flex items-center gap-4">
        <span className="text-sm font-medium">
          {selectedIds.length} document{selectedIds.length !== 1 ? 's' : ''} selected
        </span>
        <Button onClick={handleBulkDownload} className="gap-2">
          <Download className="h-4 w-4" />
          Download ZIP
        </Button>
        <Button variant="ghost" size="icon" onClick={onClear}>
          <X className="h-4 w-4" />
        </Button>
      </div>
    </Card>
  );
};
